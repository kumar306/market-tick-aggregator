package book

type OrderBookSide interface {
	// insert new price in my book side or update size for an existing price
	Upsert(price float64, quantity float64)
	// if size became 0 for a price, i will delete it from my book
	Delete(price float64)
	// each snapshot to redis/kafka will contain the best price
	Best() (priceLevel *PriceLevel, exists bool)
	// top N will use Iterator() within to get the top N price levels and publish to kafka downstream
	TopN(n int) []*PriceLevel
	// i will use this to stream through the order book and do stuff like dumping order book for persistence
	Iterate(fn func(price float64, quantity float64) bool)
}
