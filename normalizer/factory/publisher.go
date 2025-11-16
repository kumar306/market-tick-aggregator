package factory

import (
	"market-normalizer/constants"
	"market-normalizer/kafka"
	"shared/logger"
	"strings"
	"sync"
)

var publisherRegistry = make(map[string]constants.PublisherStrategy)
var oncePublisher sync.Once

func InitPublisherRegistry() {
	channels := []string{
		constants.AggTrade,
		constants.Depth,
		constants.Level2,
		constants.Ticker,
		constants.Book,
	}

	onceNormalizer.Do(func() {
		for _, ch := range channels {
			if err := RegisterPublisher(ch); err != nil {
				logger.Log.Error("Failed to register publisher, shutting down", "channel", ch, "error", err)
				panic(err)
			}
		}
	})
}

func GetRegisteredPublisher(channel string) (constants.PublisherStrategy, error) {
	key := strings.ToLower(channel)
	if v, ok := publisherRegistry[key]; ok {
		return v, nil
	}

	return nil, logger.LogAndWrap("Could not get registered normalizer from map for key", nil, "key", key)
}

func RegisterPublisher(channel string) error {
	key := strings.ToLower(channel)
	if _, exists := publisherRegistry[key]; exists {
		return nil
	}

	publisher, err := GetPublisher(key)
	if err != nil {
		return logger.LogAndWrap("Could not register publisher", nil, "error", err)
	}
	publisherRegistry[key] = publisher
	logger.Log.Info("Registered publisher for key", "key", key)
	return nil
}

func GetPublisher(key string) (constants.PublisherStrategy, error) {
	switch key {
	case "aggtrade", "ticker":
		return &TickerPublisher{}, nil
	case "depth", "level2", "book":
		return &BookPublisher{}, nil
	default:
		return nil, logger.LogAndWrap("Could not find pubilsher for given key", nil, "key", key)
	}
}

type TickerPublisher struct{}

func (t *TickerPublisher) PublishTopic() string {
	return constants.NormalizedTickerTopic
}

// input is proto byte stream
// uses kafka client - to publish to its relevant topic
func (t *TickerPublisher) Publish(raw, partitionKey []byte, exchange, channel string) {
	kafka.ProduceAsync(t.PublishTopic(), exchange, channel, partitionKey, raw)
}

type BookPublisher struct{}

func (b *BookPublisher) PublishTopic() string {
	return constants.NormalizedBookTopic
}

func (b *BookPublisher) Publish(raw, partitionKey []byte, exchange, channel string) {
	kafka.ProduceAsync(b.PublishTopic(), exchange, channel, partitionKey, raw)
}
