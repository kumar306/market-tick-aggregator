package orderedtree

// iterate N nodes in the strcture find the best N prices per side for snapshot
type Iterator interface {
	Valid() bool
	Key() float64
	Value() float64
	Next()
}

type SkipListIterator struct {
	curr *node
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

func (it *SkipListIterator) Next() {
	it.curr = it.curr.forward[0]
}
