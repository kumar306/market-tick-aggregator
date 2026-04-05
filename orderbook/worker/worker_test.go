package worker

import (
	"context"
	"market-orderbook/book"
	"market-orderbook/constants"
	"market-orderbook/proto/generated"
	"shared/metrics"
	"sync"
	"testing"
	"time"
)

var metricsOnce sync.Once

func initMetrics() {
	metricsOnce.Do(func() {
		metrics.InitOrderbookMetrics()
	})
}

func newTestWorker(ctx context.Context) *Worker {
	updateCh := make(chan *constants.DispatchRecord, 10)
	ackCh := make(chan *constants.Ack, 10)
	updateAckCh := make(chan *constants.Ack, 10)
	w := NewWorker(1, ctx, 10, 15, updateCh, ackCh, updateAckCh)
	w.FlushDepth = 5
	return w
}

func TestProcessBookUpdateUpdatesState(t *testing.T) {
	initMetrics()
	ctx := context.Background()
	w := newTestWorker(ctx)

	key := "binance:BTC-USD"
	state := &SymbolState{
		Exchange:            "binance",
		Symbol:              "BTC-USD",
		Orderbook:           book.NewOrderBook(),
		LastCommittedOffset: map[int32]int64{},
		LastProcessedOffset: map[int32]int64{},
	}
	w.OrderbookStateMap[key] = state

	rec := &constants.DispatchRecord{
		Partition: 2,
		Offset:    42,
		TsMs:      1700,
		Exchange:  "binance",
		Symbol:    "BTC-USD",
		Update: &generated.NormalizedBook{
			Exchange:        "binance",
			Symbol:          "BTC-USD",
			EventTimeMillis: 1700,
			Bids: []*generated.NormalizedBook_BookLevel{
				{Price: 100, Volume: 1},
			},
			Asks: []*generated.NormalizedBook_BookLevel{
				{Price: 101, Volume: 2},
			},
		},
	}

	w.ProcessBookUpdate(rec)

	bestBid, ok := state.Orderbook.Bids.Best()
	if !ok || bestBid == nil {
		t.Fatalf("expected best bid to exist, got ok=%v", ok)
	}
	if bestBid.Price != 100 || bestBid.Quantity != 1 {
		t.Fatalf("expected best bid=(100,1), got (%v,%v)", bestBid.Price, bestBid.Quantity)
	}
	bestAsk, ok := state.Orderbook.Asks.Best()
	if !ok || bestAsk == nil {
		t.Fatalf("expected best ask to exist, got ok=%v", ok)
	}
	if bestAsk.Price != 101 || bestAsk.Quantity != 2 {
		t.Fatalf("expected best ask=(101,2), got (%v,%v)", bestAsk.Price, bestAsk.Quantity)
	}

	if state.TimestampMillis != 1700 {
		t.Fatalf("expected TimestampMillis=1700, got %d", state.TimestampMillis)
	}

	if state.LastProcessedOffset[2] != 42 {
		t.Fatalf("expected LastProcessedOffset[2]=42, got %d", state.LastProcessedOffset[2])
	}
}

func TestHandleSnapshotRequestClonesState(t *testing.T) {
	initMetrics()
	ctx := context.Background()
	w := newTestWorker(ctx)

	key := "coinbase:ETH-USD"
	state := &SymbolState{
		Exchange:            "coinbase",
		Symbol:              "ETH-USD",
		Orderbook:           book.NewOrderBook(),
		LastCommittedOffset: map[int32]int64{0: 9},
		LastProcessedOffset: map[int32]int64{0: 10},
	}
	state.Orderbook.Bids.Upsert(200, 1)
	state.Orderbook.Asks.Upsert(201, 2)
	w.OrderbookStateMap[key] = state

	w.HandleSnapshotRequest()

	snapshot, ok := w.SnapshotStateMap[key]
	if !ok || snapshot == nil {
		t.Fatalf("expected snapshot to be created for key %s", key)
	}
	if !state.SnapshotPending {
		t.Fatalf("expected SnapshotPending=true after snapshot request")
	}

	if snapshot.PartitionOffsets[0] != 10 {
		t.Fatalf("expected snapshot partition offset 10, got %d", snapshot.PartitionOffsets[0])
	}

	// ensure offsets are copied
	state.LastProcessedOffset[0] = 99
	if snapshot.PartitionOffsets[0] != 10 {
		t.Fatalf("expected snapshot offsets to be a copy, got %d", snapshot.PartitionOffsets[0])
	}

	// second request should not overwrite pending snapshot
	w.HandleSnapshotRequest()
	if len(w.SnapshotStateMap) != 1 {
		t.Fatalf("expected snapshot state map size=1, got %d", len(w.SnapshotStateMap))
	}
}

func TestEvaluateAndDispatchSnapshotGating(t *testing.T) {
	initMetrics()
	ctx := context.Background()
	w := newTestWorker(ctx)

	key := "kraken:SOL-USD"
	state := &SymbolState{
		Exchange:            "kraken",
		Symbol:              "SOL-USD",
		Orderbook:           book.NewOrderBook(),
		LastCommittedOffset: map[int32]int64{0: 9},
		LastProcessedOffset: map[int32]int64{0: 10},
		SnapshotPending:     true,
	}
	state.Orderbook.Bids.Upsert(50, 1)
	state.Orderbook.Asks.Upsert(51, 1)
	w.OrderbookStateMap[key] = state

	snapshot := w.CloneLightWeight(state.Exchange, state.Symbol, state.LastProcessedOffset)
	w.SnapshotStateMap[key] = snapshot

	// not safe yet, should not dispatch
	w.EvaluateAndDispatchSnapshot()
	select {
	case <-w.SnapshotChannel:
		t.Fatalf("did not expect snapshot to be dispatched before commit")
	default:
	}

	// now safe to snapshot
	state.LastCommittedOffset[0] = 10
	w.EvaluateAndDispatchSnapshot()
	select {
	case msg := <-w.SnapshotChannel:
		if msg.Key != key {
			t.Fatalf("expected snapshot key %s, got %s", key, msg.Key)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected snapshot to be dispatched")
	}
}

func TestHandleSnapshotPersistedCleansState(t *testing.T) {
	initMetrics()
	ctx := context.Background()
	w := newTestWorker(ctx)

	key := "bybit:ETH-USD"
	state := &SymbolState{
		Exchange:            "bybit",
		Symbol:              "ETH-USD",
		Orderbook:           book.NewOrderBook(),
		LastCommittedOffset: map[int32]int64{},
		LastProcessedOffset: map[int32]int64{},
		SnapshotPending:     true,
	}
	w.OrderbookStateMap[key] = state
	w.SnapshotStateMap[key] = &book.OrderBookSnapshot{Exchange: "bybit", Symbol: "ETH-USD"}

	w.HandleSnapshotPersisted(key)

	if state.SnapshotPending {
		t.Fatalf("expected SnapshotPending=false after persist")
	}
	if _, ok := w.SnapshotStateMap[key]; ok {
		t.Fatalf("expected snapshot state to be deleted after persist")
	}
}

func TestProcessUpdateAckUpdatesCommittedOffsets(t *testing.T) {
	initMetrics()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newTestWorker(ctx)

	key1 := "binance:BTC-USD"
	key2 := "coinbase:ETH-USD"
	w.OrderbookStateMap[key1] = &SymbolState{
		Exchange:            "binance",
		Symbol:              "BTC-USD",
		Orderbook:           book.NewOrderBook(),
		LastCommittedOffset: map[int32]int64{},
		LastProcessedOffset: map[int32]int64{},
	}
	w.OrderbookStateMap[key2] = &SymbolState{
		Exchange:            "coinbase",
		Symbol:              "ETH-USD",
		Orderbook:           book.NewOrderBook(),
		LastCommittedOffset: map[int32]int64{},
		LastProcessedOffset: map[int32]int64{},
	}

	go w.ProcessUpdateAck()

	ackMap := map[int32]int64{0: 10, 1: 20}
	w.UpdateAckChannel <- &constants.Ack{
		Epoch:            3,
		PartitionOffsets: ackMap,
	}

	select {
	case rec := <-w.UpdateChannel:
		if rec.Event != constants.SnapshotExecuteEvent {
			t.Fatalf("expected SnapshotExecuteEvent, got %v", rec.Event)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected snapshot execute event to be enqueued")
	}

	for _, st := range w.OrderbookStateMap {
		if st.LastCommittedOffset[0] != 10 || st.LastCommittedOffset[1] != 20 {
			t.Fatalf("expected committed offsets to be copied to state")
		}
	}

	// ensure copied map is not aliased
	ackMap[0] = 99
	if w.OrderbookStateMap[key1].LastCommittedOffset[0] == 99 {
		t.Fatalf("expected committed offsets to be copied, not aliased")
	}
}

func TestFlushBookSendsAckOffsets(t *testing.T) {
	initMetrics()
	ctx := context.Background()
	w := newTestWorker(ctx)

	state1 := &SymbolState{
		Exchange:            "binance",
		Symbol:              "BTC-USD",
		Orderbook:           book.NewOrderBook(),
		LastCommittedOffset: map[int32]int64{},
		LastProcessedOffset: map[int32]int64{0: 10, 1: 20},
	}
	state1.Orderbook.Bids.Upsert(100, 1)

	state2 := &SymbolState{
		Exchange:            "coinbase",
		Symbol:              "ETH-USD",
		Orderbook:           book.NewOrderBook(),
		LastCommittedOffset: map[int32]int64{},
		LastProcessedOffset: map[int32]int64{1: 25, 2: 5},
	}
	state2.Orderbook.Bids.Upsert(200, 1)

	w.OrderbookStateMap["binance:BTC-USD"] = state1
	w.OrderbookStateMap["coinbase:ETH-USD"] = state2

	w.FlushBook(7)

	select {
	case ack := <-w.AckChannel:
		if ack.Epoch != 7 {
			t.Fatalf("expected ack epoch=7, got %d", ack.Epoch)
		}
		if ack.WorkerID != w.ID {
			t.Fatalf("expected ack worker id=%d, got %d", w.ID, ack.WorkerID)
		}
		if ack.PartitionOffsets[0] != 10 || ack.PartitionOffsets[1] != 25 || ack.PartitionOffsets[2] != 5 {
			t.Fatalf("unexpected ack offsets: %+v", ack.PartitionOffsets)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected flush ack to be sent")
	}
}

func TestBestLevelsForFlushUsesActualBestPrices(t *testing.T) {
	initMetrics()

	st := &SymbolState{
		Exchange:            "binance",
		Symbol:              "BTCUSDT",
		Orderbook:           book.NewOrderBook(),
		LastCommittedOffset: map[int32]int64{},
		LastProcessedOffset: map[int32]int64{},
	}

	st.Orderbook.Bids.Upsert(100, 1)
	st.Orderbook.Bids.Upsert(101, 1)
	st.Orderbook.Bids.Upsert(102, 1)
	st.Orderbook.Asks.Upsert(103, 1)
	st.Orderbook.Asks.Upsert(104, 1)

	bestBid, bestAsk, ok := bestLevelsForFlush(st)
	if !ok {
		t.Fatalf("expected best levels to exist")
	}

	if bestBid.Price != 102 {
		t.Fatalf("expected best bid price 102, got %v", bestBid.Price)
	}

	if bestAsk.Price != 103 {
		t.Fatalf("expected best ask price 103, got %v", bestAsk.Price)
	}
}
