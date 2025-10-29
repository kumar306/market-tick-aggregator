package binance

import "market-adapter/constants"

type BinanceFactory struct{}

func (b *BinanceFactory) CreateNormalizer() constants.Normalizer {
	return &BinanceNormalizer{}
}

func (b *BinanceFactory) CreateSubscriber(channel string) constants.Subscriber {
	return &BinanceSubscriber{Channel: channel}
}

func (b *BinanceFactory) CreatePinger() constants.Pinger {
	return &BinancePinger{}
}
