package worker

import (
	"context"
	"market-orderbook/book"
	"market-orderbook/constants"
	"market-orderbook/proto/generated"
	"market-orderbook/redis"
	"shared/logger"
	"time"

	"google.golang.org/protobuf/proto"
)

// maintain a snapshot channel, flush channel, update channel per worker?
// push into worker channel - SnapshotRequest event
// this will take care of copying the orderbook, offsets into snapshot goroutine per worker at intervals

type Worker struct {
	ID  int
	Ctx context.Context

	// channel thru which book updates enter
	UpdateChannel chan *constants.DispatchRecord

	// snapshot fields
	SnapshotChannel chan int
	SnapshotDepth   int

	// on snapshot, for every symbol managed by worker, we will do snapshot
	SnapshotStateMap map[string]*book.OrderBookSnapshot

	// orderbook state for symbol
	OrderbookStateMap map[string]*SymbolState
}

type SymbolState struct {
	Exchange        string
	Symbol          string
	TimestampMillis int64

	// orderbook
	Orderbook *book.OrderBook

	// map of partition to offset for coordinated commit post flush
	LastCommittedOffset map[int32]int64
	LastProcessedOffset map[int32]int64

	Restored        bool
	SnapshotPending bool
}

func NewWorker(id int, channel chan *constants.DispatchRecord) *Worker {
	return &Worker{
		ID:                id,
		UpdateChannel:     channel,
		SnapshotChannel:   make(chan int),
		OrderbookStateMap: make(map[string]*SymbolState),
	}
}

func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done. Returning from orderbook worker loop")
			return

		case rec, ok := <-w.UpdateChannel:
			if !ok {
				logger.Log.Error("The worker channel is closed", "workerIdx", w.ID)
				return
			}
			switch rec.Event {
			case constants.ProcessEvent:
				// process record coming from dispatcher and update worker's last processed offset variable
				w.ProcessBookUpdate(rec)
			case constants.SnapshotRequestEvent:
				// this is a empty record carrying a request to copy the current orderbook state into a OrderbookSnapshot struct
				// this struct is passed into a snapshot channel (each worker has its snapshot goroutine to isolate snapshots among the workers as it is time consuming and dont want blocking channels and lag)
				// snapshot goroutine reads from its snapshot channel and persists the snapshot with already committed offset to redis
				w.HandleSnapshotRequest()
			case constants.FlushEvent:
				// this guy will persist to downstream - a bunch of orderbook states with top N prices level - to be consumed by time series db
				// upon updating the last committed offset and committing the records post flush, it sends a snapshot event to the worker channel asking it to snapshot the committed state to redis
				w.FlushBook()
			}
		}
	}
}

func (w *Worker) ProcessBookUpdate(rec *constants.DispatchRecord) {

	bufferKey := rec.Exchange + ":" + rec.Symbol

	state, exists := w.OrderbookStateMap[bufferKey]
	if !exists {
		// if the worker doesnt have an order book for the incoming symbol in memory,
		// create a empty order book
		state = w.RestoreOrCreateState(rec.Exchange, rec.Symbol)
		w.OrderbookStateMap[bufferKey] = state
	}

	// apply the latest update to state
	for _, priceLevel := range rec.Update.Bids {
		state.Orderbook.Bids.Upsert(priceLevel.Price, priceLevel.Volume)
	}

	for _, priceLevel := range rec.Update.Asks {
		state.Orderbook.Asks.Upsert(priceLevel.Price, priceLevel.Volume)
	}

	// maintain the last processed offset. on flush the last committed offset = last processed offset
	// trigger an event to snapshot channel after flush
	state.LastProcessedOffset[rec.Partition] = rec.Offset
}

func (w *Worker) FlushBook() {
	// this will persist the book to kafka
	// commit the offsets to kafka as tracked
	// will update the latest committed offset
	// enqueues a snapshot event to worker channel post committing
}

func (w *Worker) HandleSnapshotRequest() {
	// this will clone the current orderbook state into another variable with snapshotOffset metadata
	// will post it to its snapshotchannel which is read by a goroutine

	w.SnapshotStateMap = map[string]*book.OrderBookSnapshot{}

	for key, st := range w.OrderbookStateMap {
		// if the current symbol already has a snapshot pending, skip the symbol
		if st.SnapshotPending {
			continue
		}

		// clone the orderbook into a snapshot along with current snapshot offset
		clonedBook := w.CloneLightWeight(st.Exchange, st.Symbol, st.LastProcessedOffset)
		w.SnapshotStateMap[key] = clonedBook
		st.SnapshotPending = true
	}
}

func (w *Worker) SnapshotExecuteLoop() {
	for range w.SnapshotChannel {

		// check condition that snapshotOffset[partition] <= lastCommittedOffsetMap[partition]
		// so we know that the snapshot object is committed and safe to snapshot
		// we dont want to snapshot orderbook with uncommitted updates to it

		for key, snapshot := range w.SnapshotStateMap {
			if !w.OrderbookStateMap[key].SnapshotPending {
				// no snapshot is pending for this symbol
				continue
			}

			snapshotOffsetMap := snapshot.PartitionOffsets
			lastCommittedOffsetMap := w.OrderbookStateMap[key].LastCommittedOffset
			var canSnapshot bool = true
			for partition := range snapshotOffsetMap {
				if snapshotOffsetMap[partition] > lastCommittedOffsetMap[partition] {
					canSnapshot = false
					break
				}
			}

			if !canSnapshot {
				// condition not satisfied yet. it will try later and succeed
				continue
			}

			protoBids := make([]*generated.PriceLevel, 0)
			for _, bid := range snapshot.Bids {
				protoBids = append(protoBids, &generated.PriceLevel{
					Price:    bid.Price,
					Quantity: bid.Quantity,
				})
			}

			protoAsks := make([]*generated.PriceLevel, 0)
			for _, ask := range snapshot.Asks {
				protoAsks = append(protoAsks, &generated.PriceLevel{
					Price:    ask.Price,
					Quantity: ask.Quantity,
				})
			}

			snapshotProto := &generated.OrderBookSnapshot{
				Exchange:         snapshot.Exchange,
				Symbol:           snapshot.Symbol,
				PartitionOffsets: snapshot.PartitionOffsets,
				SnapshotTsMs:     snapshot.TimestampMillis,
				Bids:             protoBids,
				Asks:             protoAsks,
			}

			snapshotBytestream, marshalErr := proto.Marshal(snapshotProto)
			if marshalErr != nil {
				logger.Log.Error("Error in marshalling snapshot to protobuf", "key", key, "error", marshalErr)
				continue
			}

			redisErr := redis.LoadSnapshot(w.Ctx, key, snapshotBytestream)
			if redisErr != nil {
				logger.Log.Error("Error in loading snapshot to redis", "key", key, "error", redisErr)
				continue
			}

			w.OrderbookStateMap[key].SnapshotPending = false
			logger.Log.Info("Backed up snapshot to redis", "workerIdx", w.ID, "key", key)
		}
	}
}

func (w *Worker) RestoreOrCreateState(exchange string, symbol string) *SymbolState {
	// read redis using exchange:symbol key and fetch its bytestream value
	// read it into a orderbooksnapshot proto generated struct
	// apply the state of the orderbooksnapshot into my state

	bufferKey := exchange + ":" + symbol
	snapshotBytes, err := redis.GetSnapshot(w.Ctx, bufferKey)
	if err != nil {
		logger.Log.Error("Error in retrieving snapshot from redis for key", "key", bufferKey)
	}

	// snapshot not present
	if snapshotBytes == nil {
		return &SymbolState{
			Exchange:            exchange,
			Symbol:              symbol,
			LastCommittedOffset: map[int32]int64{},
			LastProcessedOffset: map[int32]int64{},
			Restored:            false,
			Orderbook:           book.NewOrderBook(),
		}
	}

	orderbookSnapshot := &generated.OrderBookSnapshot{}
	marshalErr := proto.Unmarshal(snapshotBytes, orderbookSnapshot)
	if marshalErr != nil {
		logger.Log.Error("Error in unmarshalling snapshot for key", "key", bufferKey, "error", marshalErr)
	}

	book := book.NewOrderBook()

	// reapply updates
	for _, lvl := range orderbookSnapshot.Bids {
		book.Bids.Upsert(lvl.Price, lvl.Quantity)
	}

	for _, lvl := range orderbookSnapshot.Asks {
		book.Asks.Upsert(lvl.Price, lvl.Quantity)
	}

	// reapply partition offset maps from snapshot
	return &SymbolState{
		Exchange:            exchange,
		Symbol:              symbol,
		TimestampMillis:     orderbookSnapshot.SnapshotTsMs,
		SnapshotPending:     false,
		LastCommittedOffset: orderbookSnapshot.PartitionOffsets,
		LastProcessedOffset: orderbookSnapshot.PartitionOffsets,
		Restored:            true,
		Orderbook:           book,
	}
}

func (w *Worker) CloneLightWeight(exchange string,
	symbol string,
	partitionOffsets map[int32]int64) *book.OrderBookSnapshot {

	key := exchange + ":" + symbol
	b := w.OrderbookStateMap[key].Orderbook

	// copy partition offsets
	copiedOffsets := make(map[int32]int64)
	for partition, offset := range partitionOffsets {
		copiedOffsets[partition] = offset
	}

	bids := make([]*book.PriceLevel, 0)
	asks := make([]*book.PriceLevel, 0)

	b.Bids.Iterate(func(price, quantity float64) bool {
		bids = append(bids, &book.PriceLevel{
			Price:    price,
			Quantity: quantity,
		})

		return true
	})

	b.Asks.Iterate(func(price, quantity float64) bool {
		asks = append(asks, &book.PriceLevel{
			Price:    price,
			Quantity: quantity,
		})

		return true
	})

	return &book.OrderBookSnapshot{
		Exchange:         exchange,
		Symbol:           symbol,
		PartitionOffsets: copiedOffsets,
		TimestampMillis:  time.Now().UnixMilli(),
		Bids:             bids,
		Asks:             asks,
	}
}
