package model

import "time"

type OrderbookFlushLevel struct {
	Exchange   string
	Symbol     string
	EventTime  time.Time
	LevelIndex int
	Side       string
	Price      float64
	Volume     float64
}
