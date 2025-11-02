package feedFactory

import (
	"market-adapter/constants"
	"market-adapter/logger"
	"market-adapter/ring"
	"sync"
)

type FeedFactory interface {
	CreateNormalizer(stream string) (constants.Normalizer, error)
	CreateSubscriber(channel string) constants.Subscriber
	CreatePinger() constants.Pinger
}

var feedRegistry = map[string]FeedFactory{}

func RegisterFeedFactory(name string, ff FeedFactory) {
	feedRegistry[name] = ff
}

func GetFeedFactory(name string) (FeedFactory, error) {
	f, ok := feedRegistry[name]
	if !ok {
		return nil, logger.LogAndWrap("No feed factory found for config", nil, "name", name)
	}
	return f, nil
}

// this function returns a stream handler object - contains normalizer, subscriber, pinger for a stream
// given the name - switch case to get the factory
func GetStreamHandler(name string, streamCfg *constants.Stream) (*constants.StreamHandler, error) {
	// init each stream's ring buffer, lifecycle
	factory, err := GetFeedFactory(name)
	if err != nil {
		return nil, logger.LogAndWrap("No feed factory for the given stream name", nil, "name", name)
	}

	normalizer, err := factory.CreateNormalizer(streamCfg.Channel)
	if err != nil {
		return nil, logger.LogAndWrap("Could not create stream handler as could not create normalizer", err, "feed_name", name, "stream_channel", streamCfg.Channel)
	}

	return &constants.StreamHandler{
		Normalizer: normalizer,
		Subscriber: factory.CreateSubscriber(streamCfg.Channel),
		Pinger:     factory.CreatePinger(),
		Ring:       ring.NewSpscDropOldestRing[[]byte](streamCfg.RingBufferSize, name+"|"+streamCfg.Channel),
		Mu:         &sync.Mutex{},
	}, nil

}
