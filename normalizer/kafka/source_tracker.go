package kafka

import (
	"context"
	"shared/logger"
	"shared/srcoffset"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TP identifies a source (consumed) partition.
type TP struct {
	Topic     string
	Partition int32
}

// SourceTracker remembers which consumed records have an output produce still waiting for its ack.
//
// In at-least-once mode there is no transaction tying "output is durable" to "input offset is committed",
// so the offset we commit must never pass a record whose output is not acked yet. This tracks the unacked
// ones. Records parked in an orderer buffer are covered separately by the worker, because they have no produce yet.
type SourceTracker struct {
	mu       sync.Mutex
	inflight map[TP]map[int64]struct{}
	failed   bool
}

func NewSourceTracker() *SourceTracker {
	return &SourceTracker{inflight: make(map[TP]map[int64]struct{})}
}

func (t *SourceTracker) add(k TP, off int64) {
	t.mu.Lock()
	offs := t.inflight[k]
	if offs == nil {
		offs = make(map[int64]struct{}, 256)
		t.inflight[k] = offs
	}
	offs[off] = struct{}{}
	t.mu.Unlock()
}

func (t *SourceTracker) done(k TP, off int64) {
	t.mu.Lock()
	delete(t.inflight[k], off)
	t.mu.Unlock()
}

// a failed produce stays in flight on purpose, so the committed offset stops in front of it and the
// record is redelivered after a restart instead of being silently lost.
func (t *SourceTracker) fail(k TP, off int64) {
	t.mu.Lock()
	first := !t.failed
	t.failed = true
	t.mu.Unlock()
	if first {
		logger.Log.Error("Produce failed in at-least-once mode; offset commits for this partition stop at the failed record until restart",
			"topic", k.Topic, "partition", k.Partition, "offset", off)
	}
}

// MinInflight returns the lowest unacked source offset per partition.
func (t *SourceTracker) MinInflight() map[TP]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[TP]int64, len(t.inflight))
	for k, offs := range t.inflight {
		first := true
		var min int64
		for off := range offs {
			if first || off < min {
				min, first = off, false
			}
		}
		if !first {
			out[k] = min
		}
	}
	return out
}

// Frontier is the offset it is safe to commit per partition: everything before it has been fully handled.
// That is the polled position, held back by the oldest unacked produce and the oldest record still buffered
// in an orderer. Entries that did not advance past last are left out, so an idle partition costs no commit.
func Frontier(uncommitted map[string]map[int32]kgo.EpochOffset, inflight, buffered, last map[TP]int64) map[string]map[int32]kgo.EpochOffset {
	out := make(map[string]map[int32]kgo.EpochOffset)
	for topic, parts := range uncommitted {
		for p, eo := range parts {
			k := TP{Topic: topic, Partition: p}
			off := eo.Offset
			if v, ok := inflight[k]; ok && v < off {
				off = v
			}
			if v, ok := buffered[k]; ok && v < off {
				off = v
			}
			if prev, ok := last[k]; ok && off <= prev {
				continue
			}
			if out[topic] == nil {
				out[topic] = make(map[int32]kgo.EpochOffset)
			}
			out[topic][p] = kgo.EpochOffset{Epoch: eo.Epoch, Offset: off}
		}
	}
	return out
}

// TrackedProducer produces on a plain (non-transactional) client and keeps the SourceTracker current.
// It satisfies constants.Producer, so the existing publishers use it unchanged.
// Produce must only be called from the worker goroutine.
type TrackedProducer struct {
	cl      *kgo.Client
	tracker *SourceTracker
	topics  map[string]string // interned, so reading the header does not allocate a topic string per record
}

func NewTrackedProducer(cl *kgo.Client, tracker *SourceTracker) *TrackedProducer {
	return &TrackedProducer{cl: cl, tracker: tracker, topics: make(map[string]string)}
}

func (p *TrackedProducer) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	k, off, tracked := p.source(r)
	if tracked {
		p.tracker.add(k, off)
	}
	p.cl.Produce(ctx, r, func(rec *kgo.Record, err error) {
		if tracked {
			if err == nil {
				p.tracker.done(k, off)
			} else {
				p.tracker.fail(k, off)
			}
		}
		promise(rec, err)
	})
}

// synthetic records (e.g. a REST snapshot) carry no source header and are not tracked.
func (p *TrackedProducer) source(r *kgo.Record) (TP, int64, bool) {
	for _, h := range r.Headers {
		if h.Key != srcoffset.HeaderKey {
			continue
		}
		topic, partition, off, ok := srcoffset.Decode(h.Value)
		if !ok {
			return TP{}, 0, false
		}
		name, seen := p.topics[string(topic)]
		if !seen {
			name = string(topic)
			p.topics[name] = name
		}
		return TP{Topic: name, Partition: partition}, off, true
	}
	return TP{}, 0, false
}
