package model

import (
	"errors"
	"time"
)

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

func IsInvalidBookFlush(t *OrderbookFlush) error {
	if t.FlushRow.EventTimeMillis == 0 {
		return errors.New("Invalid timestamp")
	}
	return nil
}
