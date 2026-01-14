package orderedtree

type OrderedTree interface {
	Insert(key float64, value float64) bool
	Delete(key float64) bool
	Get(key float64) (float64, bool)
	Min() (float64, float64, bool)
	Max() (float64, float64, bool)
	Iterator() *Iterator
}
