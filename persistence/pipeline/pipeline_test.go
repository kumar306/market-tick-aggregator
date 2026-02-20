package pipeline

import (
	"errors"
	"testing"

	"market-persistence/batcher"

	"github.com/twmb/franz-go/pkg/kgo"
)

// created fake wrappers for testing purposes
type fakeConverter[T any] struct {
	out T
	err error
}

func (f *fakeConverter[T]) Convert([]byte) (T, error) {
	return f.out, f.err
}

type fakeBatcher[T any] struct {
	addCalls int
	lastItem batcher.BatchItem[T]
}

func (f *fakeBatcher[T]) Add(item batcher.BatchItem[T]) {
	f.addCalls++
	f.lastItem = item
}

func TestProcessAddsToBatcher(t *testing.T) {
	c := &fakeConverter[string]{out: "converted"}
	b := &fakeBatcher[string]{}
	p := &Pipeline[string]{
		Name:      "tickPipeline",
		Converter: c,
		Batcher:   b,
	}

	p.Process(&kgo.Record{
		Value:     []byte("raw"),
		Partition: 3,
		Offset:    99,
	})

	if b.addCalls != 1 {
		t.Fatalf("Add calls = %d, want 1", b.addCalls)
	}
	if b.lastItem.Item != "converted" {
		t.Fatalf("added item = %q, want %q", b.lastItem.Item, "converted")
	}
	if b.lastItem.Record.Partition != 3 || b.lastItem.Record.Offset != 99 {
		t.Fatalf("unexpected partition/offset: %+v", b.lastItem)
	}
}

func TestProcessSkipsRecordOnConvertError(t *testing.T) {
	p := &Pipeline[string]{
		Name:      "tickPipeline",
		Converter: &fakeConverter[string]{err: errors.New("decode failed")},
		Batcher:   &fakeBatcher[string]{},
	}

	p.Process(&kgo.Record{Value: []byte("raw")})

	b, _ := p.Batcher.(*fakeBatcher[string])

	if b.addCalls != 0 {
		t.Fatalf("Add calls = %d, want 0", b.addCalls)
	}
}
