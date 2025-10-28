package feedFactory

import (
	"market-adapter/constants"
	"market-adapter/logger"
)

type FeedFactory interface {
	CreateNormalizer() constants.Normalizer
	CreateSubscriber() constants.Subscriber
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
