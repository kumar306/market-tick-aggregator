package factory

import (
	"market-normalizer/constants"
	"shared/logger"
	"strings"
	"sync"
)

var publisherRegistry = make(map[string]constants.PublisherStrategy)
var oncePublisher sync.Once

func GetRegisteredPublisher(channel string) (constants.PublisherStrategy, error) {
	key := strings.ToLower(channel)
	if v, ok := publisherRegistry[key]; ok {
		return v, nil
	}

	return nil, logger.LogAndWrap("Could not get registered normalizer from map for key", nil, "key", key)
}
