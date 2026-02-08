package model

import "time"

type AggregatedTick struct {
	Exchange           string
	Symbol             string
	WindowId           string
	StartTsMs          int64
	EndTsMs            int64
	StartTs            time.Time
	EndTs              time.Time
	Open               float64
	Close              float64
	High               float64
	Low                float64
	VWAP               float64
	RollingVWAP        float64
	TWAP               float64
	Microprice         float64
	Volume             float64
	RollingVolume      float64
	VolumeAcceleration float64
	Volatility         float64
	Atr                float64
	Ema                float64
	Sma                float64
	LogReturn          float64
	SimpleReturn       float64
}
