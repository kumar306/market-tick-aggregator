package orderedtree

type OrderedTree interface {
	Insert(key float64, value float64) bool
	Delete(key float64) bool
	Get(key float64) (float64, bool)
	Min() (float64, float64, bool)
	Max() (float64, float64, bool)
	Iterator() Iterator
	ReverseIterator() Iterator
	Size() int
}

// iterate N nodes in the strcture find the best N prices per side for snapshot
type Iterator interface {
	// returns true if current iterator element is not nil
	Valid() bool
	Key() float64
	Value() float64
	// go to the next element as per iterator / reverse iterator logic
	Iterate()
}
