package model

import "time"

type OrderbookFlush struct {
	Exchange        string
	Symbol          string
	EventTimeMillis int64
	EventTime       time.Time
	BestBidPrice    float64
	BestBidVolume   float64
	BestAskPrice    float64
	BestAskVolume   float64
	Spread          float64
}
