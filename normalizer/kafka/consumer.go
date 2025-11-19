package kafka

import (
	"context"
	"market-normalizer/constants"
	"os"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	client *kgo.Client
	adm    *kadm.Client
	once   sync.Once
)

// todo: take care of partition rebalancing upon revocation/allocation/consumer group modification
func Init(cfg *constants.KafkaConfig) *kgo.Client {

	once.Do(func() {
		client, err := kgo.NewClient(
			kgo.SeedBrokers(cfg.Brokers...),
			kgo.ConsumeTopics(cfg.Topics...),
			kgo.DisableAutoCommit(),
			kgo.ConsumerGroup(cfg.ConsumerGroup),
			kgo.MaxBufferedRecords(cfg.MaxBufferRecords),
		)

		if err != nil || client == nil {
			logger.Log.Error("Error in creating kafka consumer. Returning", "error", err)
			os.Exit(1)
		}

		err = client.Ping(context.Background())
		if err != nil {
			logger.Log.Error("Error in pinging from kafka consumer. Returning", "error", err)
		}

		adm = kadm.NewClient(client)

	})

	return client
}

// close the client
func Close() {
	if client != nil {
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// hangs until buffered records are flushed
		err := client.Flush(flushCtx)
		if err != nil {
			logger.Log.Warn("Kafka flush timed out or canceled", "err", err)
		}

		client.Close()
		logger.Log.Info("Kafka client closed.")
	}
}

/*
this goroutine will pull from all partitions and post into a dispatcher channel.
using a dispatcher to segregate the messages to be picked up by different workers
initially thought i could just create a goroutine out of every message that comes
but this could very quickly exhaust resources as message volume is high.

Soln would be to have a fixed number of workers to pick these messages
messages are routed to worker shard; so they can be ordered in the same shard
let worker have a map so he can handle multiple streams at once

e.g coinbase ticker messages for eth-usd for should all go to one worker.
coinbase ticker messages for btc-usd to different worker
kraken book messages for eth-usd should all go to one specific worker, etc

todo: plan a strategy such that even allocation of work takes place

passing in the shutdown context so its shut down upon SIGTERM
*/
func ConsumerLoop(ctx context.Context, client *kgo.Client, dispatchChannel chan *kgo.Record) {

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Consumer loop received shutdown. Shutting down..")
			return
		default:
		}

		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			logger.Log.Info("Kafka client is closed upon poll fetch, returning")
			return
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			// in event that channel is blocked, avoid hanging upon shutdown
			select {
			case dispatchChannel <- rec:
				metrics.Normalizer_ConsumerMessagesTotal.WithLabelValues(rec.Topic, string(rec.Partition)).Inc()
			case <-ctx.Done():
				return
			}
		})

		fetches.EachError(func(topic string, partition int32, err error) {
			logger.Log.Error("Error occurred for fetch", "topic", topic, "partition", partition, "err", err)
			metrics.Normalizer_ConsumerErrorsTotal.WithLabelValues(topic, string(partition)).Inc()
		})
	}
}

func KafkaConsumerMetrics(ctx context.Context, topics []string) {
	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// metrics to read - consumer lag = latest offset - latest committed offset
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
				metrics.Normalizer_ConsumerLag.WithLabelValues(lo.Topic, strconv.Itoa(int(lo.Partition))).Set(float64(lag))
			})

		}
	}
}
