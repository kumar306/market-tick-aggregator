package model

import "time"

type OrderbookFlush struct {
	FlushRow  *OrderbookFlushRow
	LevelRows []*OrderbookFlushLevelRow
}

type OrderbookFlushRow struct {
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

type OrderbookFlushLevelRow struct {
	LevelIndex int
	Side       string
	Price      float64
	Volume     float64
}
