package dto

import "time"

type CandleDTO struct {
	StartTs time.Time `json:"start_ts"`
	EndTs   time.Time `json:"end_ts"`
	Open    float64   `json:"open"`
	Low     float64   `json:"low"`
	High    float64   `json:"high"`
	Close   float64   `json:"close"`
	Volume  float64   `json:"volume"`
}

type MetricDTO struct {
	Window  string    `json:"window"`
	Name    string    `json:"name"`
	Value   float64   `json:"value"`
	StartTs time.Time `json:"start_ts"`
	EndTs   time.Time `json:"end_ts"`
}

type MetricResultDTO struct {
	WindowMetrics map[string][]*MetricDTO `json:"window_metrics"`
	Exchange      string                  `json:"exchange"`
	Symbol        string                  `json:"symbol"`
}

type OrderbookDTO struct {
	Exchange      string                          `json:"exchange"`
	Symbol        string                          `json:"symbol"`
	EventTime     time.Time                       `json:"event_time"`
	BestBidPrice  float64                         `json:"best_bid_price"`
	BestBidVolume float64                         `json:"best_bid_volume"`
	BestAskPrice  float64                         `json:"best_ask_price"`
	BestAskVolume float64                         `json:"best_ask_volume"`
	Spread        float64                         `json:"spread"`
	Levels        map[string][]*OrderbookLevelDTO `json:"levels"`
}

type OrderbookLevelDTO struct {
	LevelIndex int     `json:"level_index"`
	Price      float64 `json:"price"`
	Volume     float64 `json:"volume"`
}
