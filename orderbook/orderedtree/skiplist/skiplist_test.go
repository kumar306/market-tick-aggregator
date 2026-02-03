package skiplist

import (
	"math/rand"
	"testing"
)

func seedRand() {
	rand.Seed(1)
}

func TestSkipListInsertGetMinMaxDelete(t *testing.T) {
	seedRand()
	s := NewSkipList()

	if s.Size() != 0 {
		t.Fatalf("expected empty skiplist size=0, got %d", s.Size())
	}

	if !s.Insert(10, 1) {
		t.Fatalf("expected insert to return true for new key")
	}
	if !s.Insert(5, 2) {
		t.Fatalf("expected insert to return true for new key")
	}
	if !s.Insert(20, 3) {
		t.Fatalf("expected insert to return true for new key")
	}

	if s.Size() != 3 {
		t.Fatalf("expected size=3, got %d", s.Size())
	}

	val, ok := s.Get(5)
	if !ok || val != 2 {
		t.Fatalf("expected Get(5)=2, ok=true, got %v, %v", val, ok)
	}

	if s.Insert(5, 4) {
		t.Fatalf("expected insert to return false when updating existing key")
	}
	val, ok = s.Get(5)
	if !ok || val != 4 {
		t.Fatalf("expected updated Get(5)=4, ok=true, got %v, %v", val, ok)
	}

	minKey, minVal, ok := s.Min()
	if !ok || minKey != 5 || minVal != 4 {
		t.Fatalf("expected Min=(5,4), ok=true, got (%v,%v), %v", minKey, minVal, ok)
	}

	maxKey, maxVal, ok := s.Max()
	if !ok || maxKey != 20 || maxVal != 3 {
		t.Fatalf("expected Max=(20,3), ok=true, got (%v,%v), %v", maxKey, maxVal, ok)
	}

	if !s.Delete(5) {
		t.Fatalf("expected delete existing key to return true")
	}
	if s.Size() != 2 {
		t.Fatalf("expected size=2 after delete, got %d", s.Size())
	}

	if !s.Delete(10) || !s.Delete(20) {
		t.Fatalf("expected deletes to return true")
	}

	if s.Size() != 0 {
		t.Fatalf("expected size=0 after deleting all, got %d", s.Size())
	}

	if _, _, ok := s.Min(); ok {
		t.Fatalf("expected Min to return ok=false on empty list")
	}

	if _, _, ok := s.Max(); ok {
		t.Fatalf("expected Max to return ok=false on empty list")
	}
}

func TestSkipListIteratorsOrder(t *testing.T) {
	seedRand()
	s := NewSkipList()

	keys := []float64{10, 5, 20, 15}
	for _, k := range keys {
		s.Insert(k, k*10)
	}

	expectedAsc := []float64{5, 10, 15, 20}
	it := s.Iterator()
	idx := 0
	for it.Valid() {
		if idx >= len(expectedAsc) {
			t.Fatalf("iterator returned more items than expected")
		}
		if it.Key() != expectedAsc[idx] {
			t.Fatalf("iterator order mismatch at %d: got %v, expected %v", idx, it.Key(), expectedAsc[idx])
		}
		it.Iterate()
		idx++
	}
	if idx != len(expectedAsc) {
		t.Fatalf("iterator returned %d items, expected %d", idx, len(expectedAsc))
	}

	expectedDesc := []float64{20, 15, 10, 5}
	rit := s.ReverseIterator()
	idx = 0
	for rit.Valid() {
		if idx >= len(expectedDesc) {
			t.Fatalf("reverse iterator returned more items than expected")
		}
		if rit.Key() != expectedDesc[idx] {
			t.Fatalf("reverse iterator order mismatch at %d: got %v, expected %v", idx, rit.Key(), expectedDesc[idx])
		}
		rit.Iterate()
		idx++
	}
	if idx != len(expectedDesc) {
		t.Fatalf("reverse iterator returned %d items, expected %d", idx, len(expectedDesc))
	}
}
