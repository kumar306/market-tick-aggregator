package skiplist

type SkipListReverseIterator struct {
	curr *Node
}

func (it *SkipListReverseIterator) Valid() bool {
	return it.curr != nil && it.curr.prev != nil
}

func (it *SkipListReverseIterator) Key() float64 {
	return it.curr.key
}

func (it *SkipListReverseIterator) Value() float64 {
	return it.curr.value
}

func (it *SkipListReverseIterator) Iterate() {
	it.Prev()
}

func (it *SkipListReverseIterator) Prev() {
	it.curr = it.curr.prev
}
