package binance

import "market-adapter/constants"

type BinanceFactory struct{}

func (b *BinanceFactory) CreateNormalizer() constants.Normalizer {
	return &BinanceNormalizer{}
}

func (b *BinanceFactory) CreateSubscriber() constants.Subscriber {
	return &BinanceSubscriber{}
}

func (b *BinanceFactory) CreatePinger() constants.Pinger {
	return &BinancePinger{}
}
