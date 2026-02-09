package kafka

import (
	"context"
	"market-persistence/config"
	"os"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Processor defines the interface for processing Kafka records. cannot import pipeline as import cycle
type Processor interface {
	Process(rec *kgo.Record)
}

var (
	Client        *kgo.Client
	adm           *kadm.Client
	once          sync.Once
	TickTopic     string
	BookTopic     string
	ConsumerGroup string
)

func Init(ctx context.Context, cfg *config.KafkaConfig) {
	once.Do(func() {
		client, err := kgo.NewClient(
			kgo.SeedBrokers(cfg.BootstrapServers...),
			kgo.ConsumeTopics(cfg.TopicConfig.Tick, cfg.TopicConfig.Book),
			kgo.ConsumerGroup(cfg.ConsumerGroup),
			kgo.MaxBufferedRecords(cfg.MaxBufferRecords),
			kgo.DisableAutoCommit(),
		)

		Client = client
		TickTopic = cfg.TopicConfig.Tick
		BookTopic = cfg.TopicConfig.Book
		ConsumerGroup = cfg.ConsumerGroup
		adm = kadm.NewClient(Client)
		if err != nil || client == nil {
			logger.Log.Error("Error in creating kafka consumer. Returning", "error", err)
			os.Exit(1)
		}

		err = Client.Ping(ctx)
		if err != nil {
			logger.Log.Error("Error in pinging from kafka consumer. Returning", "error", err)
		}

	})
}

func Close() {
	// flush all buffered records before i call client.close
	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := Client.Flush(flushCtx)
	if err != nil {
		logger.Log.Warn("Kafka flush timed out or canceled", "err", err)
	}

	Client.Close()
	logger.Log.Info("Kafka client closed.")
}

func StartConsumer(ctx context.Context,
	tickPipeline Processor,
	bookPipeline Processor) {
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received done event in kafka orderbook consumer loop. Exiting")
			return
		default:
		}

		fetches := Client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			logger.Log.Info("Kafka client is closed upon poll fetch, returning")
			return
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			select {
			case <-ctx.Done():
				return
			default:
			}

			metrics.Persistence_KafkaRecordsConsumed.WithLabelValues(string(rec.Partition)).Inc()
			switch rec.Topic {
			case config.AggregatedTicksTopic:
				tickPipeline.Process(rec)
			case config.OrderbookFlushesTopic:
				bookPipeline.Process(rec)
			}
		})

		fetches.EachError(func(topic string, partition int32, err error) {
			logger.Log.Info("Error occurred in fetch", "topic", topic, "partition", partition, "err", err)
			metrics.Persistence_KafkaErrorsTotal.WithLabelValues(string(partition)).Inc()
		})
	}
}

func CommitOffsetsPostWrite(ctx context.Context, topic string, partitionOffsetMap map[int32]int64) {

	metrics.Persistence_OffsetCommitAttempts.WithLabelValues(topic).Inc()

	uncommitted := make(map[string]map[int32]kgo.EpochOffset)
	uncommitted[topic] = make(map[int32]kgo.EpochOffset)

	for p, o := range partitionOffsetMap {
		uncommitted[topic][p] = kgo.EpochOffset{
			// written in doc to do Offset: the offset to read next from. so inc 1
			Offset: o + 1,
			Epoch:  -1,
		}
	}

	// if kafka offset fails, it will read same offset from kafka again later
	// db write is idempotent so on retry it will commit
	Client.CommitOffsets(ctx, uncommitted, func(c *kgo.Client, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			logger.Log.Error("Kafka offset commit failed. Will replay the log and try to commit again")
			metrics.Persistence_OffsetCommitFailures.WithLabelValues(topic).Inc()
			return
		}

		for _, topic := range resp.Topics {
			for _, partition := range topic.Partitions {
				if partition.ErrorCode != 0 {
					logger.Log.Error("Partition commit failed",
						"partition", partition.Partition,
						"error", kerr.ErrorForCode(partition.ErrorCode))
					metrics.Persistence_OffsetCommitFailures.WithLabelValues(topic.Topic).Inc()
					return
				}

				committedOffset := req.Topics[0].Partitions[partition.Partition].Offset - 1
				metrics.Persistence_OffsetCommitted.WithLabelValues(topic.Topic, strconv.Itoa(int(partition.Partition))).Set(float64(committedOffset))
			}
		}

		logger.Log.Info("Committed offsets successfully")
		metrics.Persistence_OffsetCommitSuccess.WithLabelValues(topic).Inc()
	})
}

func RecordConsumerLag(ctx context.Context, topics []string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done event. Returning from record consumer lag loop")
			return
		case <-ticker.C:
			latestOffsets, err := adm.ListEndOffsets(ctx, topics...)
			if err != nil {
				logger.Log.Error("Error fetching latest offsets", "err", err)
				continue
			}
			committedOffsets, err := adm.ListCommittedOffsets(ctx, topics...)
			if err != nil {
				logger.Log.Error("Error fetching committed offsets", "err", err)
				continue
			}

			latestOffsets.Each(func(lo kadm.ListedOffset) {
				latest := lo.Offset
				var lastCommittedOffset int64
				// coudn't find offset
				if latest == -1 {
					latest = 0
				}

				val, exists := committedOffsets.Lookup(lo.Topic, lo.Partition)
				lastCommittedOffset = val.Offset

				if !exists || lastCommittedOffset < 0 {
					lastCommittedOffset = 0
				}

				lag := latest - lastCommittedOffset
				metrics.Persistence_ConsumerLag.WithLabelValues(lo.Topic, strconv.Itoa(int(lo.Partition))).Set(float64(lag))
			})
		}
	}
}
