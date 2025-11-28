package kafka

import (
	"context"
	"fmt"
	"market-normalizer/constants"
	"os"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"sync"
	"time"

	"github.com/sony/gobreaker"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type ProduceCBResponse struct {
	isError bool
	err     error
}

var (
	Client         *kgo.Client
	adm            *kadm.Client
	once           sync.Once
	KafkaBreaker   *gobreaker.CircuitBreaker
	producerErrors chan error
	replayLock     sync.RWMutex
	ReplayDone     chan struct{}
)

// todo: take care of partition rebalancing upon revocation/allocation/consumer group modification
func Init(ctx context.Context, cfg *constants.KafkaConfig) *kgo.Client {

	once.Do(func() {
		client, err := kgo.NewClient(
			kgo.SeedBrokers(cfg.Brokers...),
			kgo.ConsumeTopics(cfg.Topics...),
			kgo.DisableAutoCommit(),
			kgo.ConsumerGroup(cfg.ConsumerGroup),
			kgo.MaxBufferedRecords(cfg.MaxBufferRecords),
		)

		Client = client

		if err != nil || Client == nil {
			logger.Log.Error("Error in creating kafka consumer. Returning", "error", err)
			os.Exit(1)
		}

		err = Client.Ping(ctx)
		if err != nil {
			logger.Log.Error("Error in pinging from kafka consumer. Returning", "error", err)
		}

		adm = kadm.NewClient(Client)

		KafkaBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        "kafka-circuit-breaker",
			Timeout:     time.Duration(cfg.CBTimeoutMillis) * time.Millisecond,
			MaxRequests: 0,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				if counts.Requests < uint32(cfg.CBReqCount) {
					return false
				}

				return counts.ConsecutiveFailures >= uint32(cfg.CBConsecutiveFailures)
			},
			OnStateChange: func(name string, from, to gobreaker.State) {
				logger.Log.Warn("Kafka circuit breaker state change", "name", name, "from", from.String(), "to", to.String())
				metrics.Normalizer_KafkaCB_StateChanges.WithLabelValues(to.String()).Inc()
				metrics.Normalizer_KafkaCB_State.Set(float64(to))

				if to == gobreaker.StateClosed {
					logger.Log.Info("State changed to closed. Replaying WAL if messages exists")
					errorChan := make(chan error, 1)

					if ReplayDone == nil {
						ReplayDone = make(chan struct{})
					}

					go func() {

						// to ensure all workers wait until the wal buffer is flushed
						replayLock.Lock()
						defer replayLock.Unlock()

						flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()

						if err := Client.Flush(flushCtx); err != nil {
							// error occurred when flushing the just queued entries. kafka clearly not healthy. so dont replay
							logger.Log.Error("Error occurred when flushing entries before replay. Stopping replay", "err", err)
							return
						}

						Wal.Replay(func(entry WALEntry) error {

							rec := &kgo.Record{
								Key:   entry.Key,
								Value: entry.Value,
								Topic: entry.Topic,
							}

							Client.Produce(ctx, rec, func(r *kgo.Record, err error) {
								if err != nil {
									errorChan <- err
								} else {
									errorChan <- nil
									Client.MarkCommitRecords(entry.Msg.Record)
								}
							})

							return <-errorChan
						})

						logger.Log.Info("onstatechange: closing ReplayDone", "addr", fmt.Sprintf("%p", ReplayDone), "value", ReplayDone)
						close(ReplayDone)

					}()
				}
			},
		})

		producerErrors = make(chan error, cfg.CBProducerErrorBufferSize)
	})

	return Client
}

// close the client
func Close() {
	if Client != nil {
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// hangs until buffered records are flushed
		err := Client.Flush(flushCtx)
		if err != nil {
			logger.Log.Warn("Kafka flush timed out or canceled", "err", err)
		}

		Client.Close()
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
