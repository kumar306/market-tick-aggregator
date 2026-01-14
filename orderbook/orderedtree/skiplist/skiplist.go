package skiplist

import (
	"market-orderbook/orderedtree"
	"math/rand"
)

// this file will contain the skip list implementation
const (
	maxLevel int     = 16
	p        float64 = 0.5
)

type Node struct {
	key     float64
	value   float64
	forward []*Node
	// for prev pointer - for fast topN for bid side - i will have this != nil only at level 0
	prev *Node
}

type SkipList struct {
	// dummy node of the skip list containing pointers to all levels
	head *Node
	// tail points to current maximum value node
	tail *Node
	// current level of the list
	level int
	// total number of nodes in the list
	size int
}

func NewSkipList() *SkipList {
	head := &Node{
		forward: make([]*Node, maxLevel),
	}

	return &SkipList{
		head:  head,
		level: 1,
	}
}

// coin flipping to decide a node's level
func randomLevel() int {
	lvl := 1
	for rand.Float64() < p && lvl < maxLevel {
		lvl++
	}
	return lvl
}

func (s *SkipList) find(key float64, update []*Node) *Node {
	curr := s.head
	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].key < key {
			curr = curr.forward[i]
		}

		if update != nil {
			update[i] = curr
		}
	}

	return curr.forward[0]
}

func (s *SkipList) Size() int {
	return s.size
}

func (s *SkipList) Get(key float64) (float64, bool) {
	node := s.find(key, nil)
	if node != nil && node.key == key {
		return node.value, true
	}
	return 0, false
}

// upsert. first search for key. if it already exists, then update its value
func (s *SkipList) Insert(key float64, value float64) bool {
	update := make([]*Node, maxLevel)
	findRes := s.find(key, update)

	if findRes != nil && findRes.key == key {
		findRes.value = value
		return false
	}

	lvl := randomLevel()

	// if lvl increases with this insertion, new node added is the first node on my new levels
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	newNode := &Node{
		key:     key,
		value:   value,
		forward: make([]*Node, lvl),
	}

	if s.size == 0 {
		s.head.forward[0] = newNode
		newNode.prev = s.head
		s.tail = newNode
		s.size++
		return true
	}

	// prev pointer only at 0th level
	nextNode := update[0].forward[0]

	for i := 0; i < lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	if nextNode != nil {
		nextNode.prev = newNode
	} else {
		s.tail = newNode
	}

	newNode.prev = update[0]

	s.size++
	return true
}

func (s *SkipList) Delete(key float64) bool {
	update := make([]*Node, maxLevel)
	findRes := s.find(key, update)

	// cannot delete a node which doesnt exist
	if findRes == nil || findRes.key != key {
		return false
	}

	// now delete on all levels it exists on
	for i := 0; i < s.level; i++ {
		if update[i].forward[i] != findRes {
			// node is not present on this level or above it
			break
		}
		update[i].forward[i] = findRes.forward[i]
	}

	// reset prev at level 0 for next node
	if findRes.forward[0] != nil {
		findRes.forward[0].prev = update[0]
	} else {
		s.tail = update[0]
	}

	for s.level > 1 && s.head.forward[s.level-1] == nil {
		// if the deleted node was the only node on its level above
		s.level--
	}

	s.size--
	return true
}

func (s *SkipList) Min() (float64, float64, bool) {
	found := s.head.forward[0]
	if found == nil {
		return 0, 0, false
	}
	return found.key, found.value, true
}

func (s *SkipList) Max() (float64, float64, bool) {
	curr := s.head
	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil {
			curr = curr.forward[i]
		}
	}

	if curr == s.head {
		return 0, 0, false
	}

	return curr.key, curr.value, true
}

func (s *SkipList) Iterator() orderedtree.Iterator {
	return &SkipListIterator{
		curr: s.head.forward[0],
	}
}

func (s *SkipList) ReverseIterator() orderedtree.Iterator {
	return &SkipListReverseIterator{
		curr: s.tail,
	}
}
