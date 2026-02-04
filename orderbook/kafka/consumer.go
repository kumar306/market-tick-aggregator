package kafka

import (
	"context"
	"market-orderbook/backpressure"
	"market-orderbook/constants"
	"os"
	"shared/logger"
	"shared/metrics"
	"sync"
	"time"

	"github.com/sony/gobreaker"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	Client                  *kgo.Client
	once                    sync.Once
	UpstreamTopic           string
	DownstreamTopic         string
	ConsumerGroup           string
	ProducerErrors          chan error
	KafkaBreaker            *gobreaker.CircuitBreaker
	KafkaBreakerTestingHook func()
)

func Init(ctx context.Context, cfg *constants.KafkaConfig) {
	once.Do(func() {
		client, err := kgo.NewClient(
			kgo.SeedBrokers(cfg.BootstrapServers...),
			kgo.ConsumeTopics(cfg.TopicConfig.Upstream),
			kgo.ConsumerGroup(cfg.ConsumerGroup),
			kgo.MaxBufferedRecords(cfg.MaxBufferRecords),
			kgo.DisableAutoCommit(),
		)
		Client = client
		DownstreamTopic = cfg.TopicConfig.Downstream
		UpstreamTopic = cfg.TopicConfig.Upstream
		ConsumerGroup = cfg.ConsumerGroup
		if err != nil || client == nil {
			logger.Log.Error("Error in creating kafka consumer. Returning", "error", err)
			os.Exit(1)
		}

		err = Client.Ping(ctx)
		if err != nil {
			logger.Log.Error("Error in pinging from kafka consumer. Returning", "error", err)
		}

		UpstreamTopic = cfg.TopicConfig.Upstream
		DownstreamTopic = cfg.TopicConfig.Downstream

		KafkaBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name: "kafka-cb",
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				if counts.Requests < uint32(cfg.CBReqCount) {
					return false
				}
				failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
				return failureRatio >= cfg.CBFailureRatio
			},
			OnStateChange: func(name string, from, to gobreaker.State) {
				logger.Log.Info("Aggregator kafka cb changed states", "from", from, "to", to)
			},
		})

		ProducerErrors = make(chan error, cfg.ProduceErrorBufferSize)
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

func StartConsumer(ctx context.Context, dispatchChannel chan *kgo.Record) {
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
			case dispatchChannel <- rec:
				metrics.Orderbook_ConsumerSuccessesTotal.WithLabelValues(string(rec.Partition)).Inc()
			case <-ctx.Done():
				return
			}
		})

		fetches.EachError(func(topic string, partition int32, err error) {
			logger.Log.Info("Error occurred in fetch", "topic", topic, "partition", partition, "err", err)
			metrics.Orderbook_ConsumerErrorsTotal.WithLabelValues(string(partition)).Inc()
		})
	}
}

func RunConsumerBackpressure(ctx context.Context, client *kgo.Client) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var paused bool = false

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done event in consumer backpressure loop. Returning")
			return
		case <-ticker.C:
			state := backpressure.GetBackpressureState()

			// if state is throttling and paused is false
			if state == constants.Throttling && !paused {
				client.PauseFetchTopics(UpstreamTopic)
				paused = true
				logger.Log.Info("Current state - throttling. Paused fetch for upstream topic", "topic", UpstreamTopic)
				metrics.Orderbook_KafkaFetchPaused.Set(1.0)
			}

			// if fetch was paused and now state is healthy
			if state == constants.Healthy && paused {
				client.ResumeFetchTopics(UpstreamTopic)
				paused = false
				logger.Log.Info("Current state - healthy. Resumed fetch for upstream topic", "topic", UpstreamTopic)
				metrics.Orderbook_KafkaFetchPaused.Set(0.0)
			}
		}
	}
}
