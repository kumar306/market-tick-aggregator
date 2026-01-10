package constants

type Side int

const (
	Bid Side = iota
	Ask
)

type PriceLevel struct {
	Price    float64
	Quantity float64
}

type OrderBookSide interface {
	// insert new price in my book side or update size for an existing price
	Upsert(price float64, quantity float64)
	// if size became 0 for a price, i will delete it from my book
	Delete(price float64)
	// each snapshot to redis/kafka will contain the best price
	Best() (price float64, quantity float64, exists bool)
	// top N will use Iterator() within to get the top N price levels and publish to kafka downstream
	TopN(n int) []*PriceLevel
	// i will use this to stream through the order book and do stuff like dumping order book for persistence
	Iterate(fn func(price float64, quantity float64) bool)
}

// skip list implementation of an order book side
type TreeBookSide struct {
	Side     Side
	TreeBook *OrderedTree
}

type OrderedTree interface {
	Insert(key float64, value float64) bool
	Delete(key float64) bool
	Get(key float64) bool
	Min() (float64, float64, bool)
	Max() (float64, float64, bool)
	Iterator() *Iterator
}

// iterate N nodes in the skip list to find the best N prices per side for snapshot
type Iterator interface {
	HasNext() bool
	Key() float64
	Value() float64
	Next()
}

// order book internally stores full depth but display top N to UI for friendly interface
type OrderBook struct {
	Exchange        string
	Symbol          string
	TimestampMillis int64
	Bids            *OrderBookSide
	Asks            *OrderBookSide
}

// show top N price levels
type OrderBookSnapshot struct {
	Exchange        string
	Symbol          string
	TimestampMillis int64
	Bids            []*PriceLevel
	Asks            []*PriceLevel
}
