package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"market-ui-backend/constants"
	"market-ui-backend/proto/generated"
	"os"
	"shared/logger"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

var (
	Client     *kgo.Client
	once       sync.Once
	TicksTopic string
	BookTopic  string
)

func Init(ctx context.Context, cfg *constants.KafkaConfig) {
	once.Do(func() {
		// auto commit is enabled here
		client, err := kgo.NewClient(
			kgo.SeedBrokers(cfg.BootstrapServers...),
			kgo.ConsumeTopics(cfg.TopicConfig.Ticks, cfg.TopicConfig.Book),
			kgo.ConsumerGroup(cfg.ConsumerGroup),
			kgo.MaxBufferedRecords(cfg.MaxBufferRecords),
			// kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelDebug, nil)),
		)
		Client = client
		if err != nil || client == nil {
			logger.Log.Error("Error in creating kafka consumer. Returning", "error", err)
			os.Exit(1)
		}

		err = Client.Ping(ctx)
		if err != nil {
			logger.Log.Error("Error in pinging from kafka consumer. Returning", "error", err)
		}

		TicksTopic = cfg.TopicConfig.Ticks
		BookTopic = cfg.TopicConfig.Book
		logger.Log.Info("Initalized UI Kafka consumer")
	})
}

func StartConsumer(client *kgo.Client) {
	go func() {
		for {
			fetches := client.PollFetches(context.Background())
			if fetches.IsClientClosed() {
				return
			}

			fetches.EachRecord(func(r *kgo.Record) {
				var (
					key string
					msg []byte
					err error
					typ string
				)

				switch r.Topic {
				case TicksTopic:
					key, msg, err = parseTick(r.Value)
					typ = "tick"
				case BookTopic:
					key, msg, err = parseBook(r.Value)
					typ = "book"
				default:
					logger.Log.Warn("Skipping unknown topic record", "topic", r.Topic)
					return
				}

				if err != nil {
					logger.Log.Error("Skipping invalid kafka record", "topic", r.Topic, "error", err)
					return
				}

				Manager.Broadcast(typ, key, msg)
			})
		}
	}()
}

func parseTick(msg []byte) (string, []byte, error) {
	tick := &generated.AggregatedTick{}
	if err := proto.Unmarshal(msg, tick); err != nil {
		return "", nil, fmt.Errorf("unmarshal aggregated tick: %w", err)
	}

	key := tick.GetExchange() + ":" + tick.GetSymbol()
	if key == ":" {
		return "", nil, fmt.Errorf("missing exchange/symbol in tick message")
	}

	tick.MessageType = "tick"

	jsonBytes, err := json.Marshal(tick)
	if err != nil {
		return "", nil, fmt.Errorf("marshal tick to json: %w", err)
	}

	return key, jsonBytes, nil
}

func parseBook(msg []byte) (string, []byte, error) {
	book := &generated.OrderbookFlush{}
	if err := proto.Unmarshal(msg, book); err != nil {
		return "", nil, fmt.Errorf("unmarshal orderbook flush: %w", err)
	}

	key := book.GetExchange() + ":" + book.GetSymbol()
	if key == ":" {
		return "", nil, fmt.Errorf("missing exchange/symbol in book message")
	}

	book.MessageType = "book"

	jsonBytes, err := json.Marshal(book)
	if err != nil {
		return "", nil, fmt.Errorf("marshal book to json: %w", err)
	}

	return key, jsonBytes, nil

}
