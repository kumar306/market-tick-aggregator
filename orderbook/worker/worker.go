package worker

import (
	"context"
	"market-orderbook/book"
	"market-orderbook/constants"
	"market-orderbook/proto/generated"
	"market-orderbook/redis"
	"shared/logger"

	"google.golang.org/protobuf/proto"
)

// maintain a snapshot channel, flush channel, update channel per worker?
// push into worker channel - SnapshotRequest event
// this will take care of copying the orderbook, offsets into snapshot goroutine per worker at intervals
//

type Worker struct {
	ID                  int
	Ctx                 context.Context
	UpdateChannel       chan *constants.DispatchRecord
	SnapshotChannel     chan int
	OrderbookStateMap   map[string]*SymbolState
	LastCommittedOffset int64
	LastProcessedOffset int64
}

type SymbolState struct {
	Orderbook           *book.OrderBook
	lastCommittedOffset int64
	lastProcessedOffset int64
	restored            bool
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
			case constants.SnapshotEvent:
				// this is a empty record carrying a request to copy the current orderbook state into a OrderbookSnapshot struct
				// this struct is passed into a snapshot channel (each worker has its snapshot goroutine to isolate snapshots among the workers as it is time consuming and dont want blocking channels and lag)
				// snapshot goroutine reads from its snapshot channel and persists the snapshot with already committed offset to redis
			case constants.FlushEvent:
				// this guy will persist to downstream - a bunch of orderbook states with top N prices level - to be consumed by time series db
				// upon updating the last committed offset and committing the records post flush, it sends a snapshot event to the worker channel asking it to snapshot the committed state to redis
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
	state.lastProcessedOffset = rec.Offset
}

func (w *Worker) FlushBook() {
	// this will persist the book to kafka
	// commit the offsets to kafka as tracked
	// will update the latest committed offset
	// enqueues a snapshot event to worker channel post committing
}

func (w *Worker) SnapshotBook() {
	// this will clone the current orderbook state into another variable with snapshotOffset metadata
	// will post it to its snapshotchannel which is read by a goroutine
	// async update to redis
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
			lastCommittedOffset: -1,
			lastProcessedOffset: -1,
			restored:            false,
			Orderbook:           book.NewOrderBook(exchange, symbol),
		}
	}

	orderbookSnapshot := &generated.OrderBookSnapshot{}
	marshalErr := proto.Unmarshal(snapshotBytes, orderbookSnapshot)
	if marshalErr != nil {
		logger.Log.Error("Error in unmarshalling snapshot for key", "key", bufferKey, "error", marshalErr)
	}

	book := book.NewOrderBook(exchange, symbol)

	// reapply updates
	for _, lvl := range orderbookSnapshot.Bids {
		book.Bids.Upsert(lvl.Price, lvl.Quantity)
	}

	for _, lvl := range orderbookSnapshot.Asks {
		book.Asks.Upsert(lvl.Price, lvl.Quantity)
	}

	return &SymbolState{
		lastCommittedOffset: orderbookSnapshot.Offset,
		lastProcessedOffset: orderbookSnapshot.Offset,
		restored:            true,
		Orderbook:           book,
	}
}
