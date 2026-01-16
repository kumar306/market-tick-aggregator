package book

type Side int

const (
	Bid Side = iota
	Ask
)

type PriceLevel struct {
	Price    float64
	Quantity float64
}

// order book internally stores full depth but display top N to UI for friendly interface
type OrderBook struct {
	Exchange        string
	Symbol          string
	TimestampMillis int64
	Bids            OrderBookSide
	Asks            OrderBookSide
}

type OrderBookSnapshot struct {
	Exchange        string
	Symbol          string
	Offset          int64
	TimestampMillis int64
	Bids            []*PriceLevel
	Asks            []*PriceLevel
}
