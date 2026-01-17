package book

import (
	"market-orderbook/orderedtree"
	"market-orderbook/orderedtree/skiplist"
)

// implementation of tree book side using the ordered tree interface
type TreeBookSide struct {
	side        Side
	orderedTree orderedtree.OrderedTree
}

func NewOrderBook() *OrderBook {

	bids := NewTreeBookSide(Bid)
	asks := NewTreeBookSide(Ask)

	return &OrderBook{
		Bids: bids,
		Asks: asks,
	}
}

func NewTreeBookSide(side Side) *TreeBookSide {
	return &TreeBookSide{
		side:        side,
		orderedTree: skiplist.NewSkipList(),
	}
}

func (t *TreeBookSide) Upsert(price float64, quantity float64) {
	if quantity <= 0 {
		t.orderedTree.Delete(price)
		return
	}

	t.orderedTree.Insert(price, quantity)
}

func (t *TreeBookSide) Delete(price float64) {
	t.orderedTree.Delete(price)
}

func (t *TreeBookSide) Best() (*PriceLevel, bool) {
	var price, size float64
	var ok bool
	if t.side == Bid {
		price, size, ok = t.orderedTree.Max()
	} else {
		price, size, ok = t.orderedTree.Min()
	}

	if !ok {
		return nil, false
	}

	return &PriceLevel{Price: price, Quantity: size}, true
}

func (t *TreeBookSide) TopN(n int) []*PriceLevel {
	levels := make([]*PriceLevel, 0, n)
	var it orderedtree.Iterator

	// we need least N prices
	// while levels slice < n, iterate to next. get top N levels or if tree doesnt have N levels then get everything
	if t.side == Ask {
		it = t.orderedTree.Iterator()
	} else {
		it = t.orderedTree.ReverseIterator()
	}

	for len(levels) < n && it.Valid() {
		priceLevel := &PriceLevel{
			Price:    it.Key(),
			Quantity: it.Value(),
		}
		levels = append(levels, priceLevel)
		it.Iterate()
	}

	return levels
}

func (t *TreeBookSide) Depth() int {
	return t.orderedTree.Size()
}

func (t *TreeBookSide) Iterate(fn func(price float64, quantity float64) bool) {
	// apply this function for each node
	it := t.orderedTree.Iterator()
	for it.Valid() {
		if !fn(it.Key(), it.Value()) {
			return
		}
		it.Iterate()
	}
}
