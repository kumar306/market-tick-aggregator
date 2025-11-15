package factory

import (
	"market-normalizer/constants"
	"shared/logger"
	"strings"
	"sync"
)

// normalized ticker - convert from the pipeline message into the generated proto normalized ticker
// normalized depth - similar

// binance - binanceaggtradenormalizer

var normalizerRegistry = make(map[string]constants.NormalizerStrategy)
var onceNormalizer sync.Once

func GetRegisteredNormalizer(exchange string, channel string) (constants.NormalizerStrategy, error) {
	key := strings.ToLower(exchange) + ":" + strings.ToLower(channel)
	if v, ok := normalizerRegistry[key]; ok {
		return v, nil
	}

	return nil, logger.LogAndWrap("Could not get registered normalizer from map for key", nil, "key", key)
}
