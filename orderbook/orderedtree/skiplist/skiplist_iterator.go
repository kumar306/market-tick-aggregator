package skiplist

type SkipListIterator struct {
	curr *Node
}

func (it *SkipListIterator) Valid() bool {
	return it.curr != nil
}

func (it *SkipListIterator) Key() float64 {
	return it.curr.key
}

func (it *SkipListIterator) Value() float64 {
	return it.curr.value
}

func (it *SkipListIterator) Iterate() {
	it.Next()
}

func (it *SkipListIterator) Next() {
	it.curr = it.curr.forward[0]
}
