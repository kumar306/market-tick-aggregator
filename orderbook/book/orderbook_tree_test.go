package book

import "testing"

func TestTreeBookSideUpsertDeleteBest(t *testing.T) {
	bids := NewTreeBookSide(Bid)
	bids.Upsert(100, 1)
	bids.Upsert(101, 2)
	bids.Upsert(99, 3)

	best, ok := bids.Best()
	if !ok || best == nil {
		t.Fatalf("expected best bid to exist, got ok=%v", ok)
	}
	if best.Price != 101 || best.Quantity != 2 {
		t.Fatalf("expected best bid=(101,2), got (%v,%v)", best.Price, best.Quantity)
	}

	bids.Upsert(101, 0) // delete
	best, ok = bids.Best()
	if !ok || best == nil {
		t.Fatalf("expected best bid to exist after delete, got ok=%v", ok)
	}
	if best.Price != 100 || best.Quantity != 1 {
		t.Fatalf("expected best bid=(100,1) after delete, got (%v,%v)", best.Price, best.Quantity)
	}

	asks := NewTreeBookSide(Ask)
	asks.Upsert(200, 1)
	asks.Upsert(199, 2)
	asks.Upsert(205, 3)

	best, ok = asks.Best()
	if !ok || best == nil {
		t.Fatalf("expected best ask to exist, got ok=%v", ok)
	}
	if best.Price != 199 || best.Quantity != 2 {
		t.Fatalf("expected best ask=(199,2), got (%v,%v)", best.Price, best.Quantity)
	}

	asks.Upsert(199, 0) // delete
	best, ok = asks.Best()
	if !ok || best == nil {
		t.Fatalf("expected best ask to exist after delete, got ok=%v", ok)
	}
	if best.Price != 200 || best.Quantity != 1 {
		t.Fatalf("expected best ask=(200,1) after delete, got (%v,%v)", best.Price, best.Quantity)
	}
}

func TestTreeBookSideTopNOrdering(t *testing.T) {
	bids := NewTreeBookSide(Bid)
	bids.Upsert(100, 1)
	bids.Upsert(101, 2)
	bids.Upsert(102, 3)

	bTop := bids.TopN(2)
	if len(bTop) != 2 || bTop[0].Price != 102 || bTop[1].Price != 101 {
		t.Fatalf("expected top bids [102,101], got %+v", bTop)
	}

	asks := NewTreeBookSide(Ask)
	asks.Upsert(200, 1)
	asks.Upsert(199, 2)
	asks.Upsert(201, 3)

	aTop := asks.TopN(2)
	if len(aTop) != 2 || aTop[0].Price != 199 || aTop[1].Price != 200 {
		t.Fatalf("expected top asks [199,200], got %+v", aTop)
	}
}

func TestTreeBookSideIterateAscending(t *testing.T) {
	side := NewTreeBookSide(Bid)
	side.Upsert(3, 1)
	side.Upsert(1, 1)
	side.Upsert(2, 1)

	var got []float64
	side.Iterate(func(price float64, quantity float64) bool {
		got = append(got, price)
		return true
	})

	expected := []float64{1, 2, 3}
	if len(got) != len(expected) {
		t.Fatalf("expected %d prices, got %d", len(expected), len(got))
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Fatalf("iterate order mismatch at %d: got %v, expected %v", i, got[i], expected[i])
		}
	}
}
