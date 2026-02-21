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
	Value   float64   `json:"Value"`
	StartTs time.Time `json:"start_ts"`
	EndTs   time.Time `json:"end_ts"`
}
