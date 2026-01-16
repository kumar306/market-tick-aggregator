package worker

import (
	"context"
	"market-orderbook/book"
	"market-orderbook/constants"
	"shared/logger"
)

type Worker struct {
	ID                  int
	Channel             chan *constants.DispatchRecord
	OrderbookState      map[string]*book.OrderBook
	LastCommittedOffset int64
	LastProcessedOffset int64
}

func NewWorker(id int, channel chan *constants.DispatchRecord) *Worker {
	return &Worker{
		ID:             id,
		Channel:        channel,
		OrderbookState: make(map[string]*book.OrderBook),
	}
}

func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done. Returning from orderbook worker loop")
			return

		case rec, ok := <-w.Channel:
			if !ok {
				logger.Log.Error("The worker channel is closed", "workerIdx", w.ID)
				return
			}
			switch rec.Event {
			case constants.ProcessEvent:
				// process record coming from dispatcher and update worker's last processed offset variable
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

	// if the worker doesnt have an order book for the incoming symbol in memory,
	// create a empty order book
	// read redis using exchange:symbol key and fetch its bytestream value
	// read it into a orderbooksnapshot proto generated struct
	// apply the state of the orderbooksnapshot into my state

	// apply the latest update to state like always

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
