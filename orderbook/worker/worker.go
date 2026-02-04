package worker

import (
	"context"
	"market-orderbook/book"
	"market-orderbook/constants"
	"market-orderbook/kafka"
	"market-orderbook/proto/generated"
	"market-orderbook/redis"
	"shared/logger"
	"shared/metrics"
	"strconv"
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

	FlushDepth int

	// snapshot fields
	SnapshotChannel chan *constants.SnapshotMsg
	// on snapshot, for every symbol managed by worker, we will do snapshot
	SnapshotStateMap               map[string]*book.OrderBookSnapshot
	SnapshotPrepareIntervalSeconds int

	// orderbook state for symbol
	OrderbookStateMap map[string]*SymbolState

	// ack channel for distributed commit coordinator
	AckChannel       chan *constants.Ack
	UpdateAckChannel chan *constants.Ack
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

func NewWorker(id int, ctx context.Context, channel chan *constants.DispatchRecord, AckChannel, updateAckChannel chan *constants.Ack) *Worker {
	return &Worker{
		ID:                id,
		Ctx:               ctx,
		UpdateChannel:     channel,
		SnapshotChannel:   make(chan *constants.SnapshotMsg, 10),
		OrderbookStateMap: make(map[string]*SymbolState),
		SnapshotStateMap:  make(map[string]*book.OrderBookSnapshot),
		AckChannel:        AckChannel,
		UpdateAckChannel:  updateAckChannel,
	}
}

func (w *Worker) Run() {

	go w.SnapshotPersistLoop()
	go w.ProcessUpdateAck()
	go w.RunSnapshotPrepareScheduler()

	for {
		select {
		case <-w.Ctx.Done():
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
				w.FlushBook(rec.FlushEpoch)
			case constants.SnapshotExecuteEvent:
				// segregated snapshot execute logic into worker so that only worker updates mutable snapshot state
				// to fix data race issues
				w.EvaluateAndDispatchSnapshot()
			case constants.SnapshotPersistedEvent:
				// cleanup of snapshot state in memory post redis backup
				w.HandleSnapshotPersisted(rec.BufferKey)
			}
		}
	}
}

func (w *Worker) ProcessBookUpdate(rec *constants.DispatchRecord) {
	bufferKey := rec.Exchange + ":" + rec.Symbol

	logger.Log.Info("Received process book update for key", "key", bufferKey)
	state, exists := w.OrderbookStateMap[bufferKey]
	if !exists {
		// if the worker doesnt have an order book for the incoming symbol in memory,
		// create a empty order book
		logger.Log.Info("State doesnt exist for key in worker. Restoring or creating state", "worker", w.ID, "key", bufferKey)
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

	state.TimestampMillis = rec.TsMs

	// maintain the last processed offset. on flush the last committed offset = last processed offset
	// trigger an event to snapshot channel after flush
	state.LastProcessedOffset[rec.Partition] = rec.Offset
	metrics.Orderbook_UpdatesTotal.WithLabelValues(strconv.Itoa(w.ID), state.Exchange, state.Symbol).Add(1)
}

func (w *Worker) FlushBook(flushEpoch int32) {
	// this will persist the book to kafka
	// send the last committed partition offset map to an ack channel read by coordinated committer

	// for each symbol managed by the worker,
	// create OrderbookSnapshot with N prices, spread, best bid, best ask
	// make into proto
	// publish to kafka
	// send the lastProcessedOffset to ack channel

	logger.Log.Info("Received flush event for worker, epoch", "worker", w.ID, "epoch", flushEpoch)

	for key, st := range w.OrderbookStateMap {

		protoBids := make([]*generated.OrderbookFlush_BookLevel, 0)
		protoAsks := make([]*generated.OrderbookFlush_BookLevel, 0)

		bids := st.Orderbook.Bids.TopN(w.FlushDepth)
		asks := st.Orderbook.Asks.TopN(w.FlushDepth)

		// if empty then skip
		if len(bids) == 0 || len(asks) == 0 {
			continue
		}

		for _, bid := range bids {
			protoBids = append(protoBids, &generated.OrderbookFlush_BookLevel{
				Price:  bid.Price,
				Volume: bid.Quantity,
			})
		}

		for _, ask := range asks {
			protoAsks = append(protoAsks, &generated.OrderbookFlush_BookLevel{
				Price:  ask.Price,
				Volume: ask.Quantity,
			})
		}

		bestBid := bids[len(bids)-1]
		bestAsk := asks[0]
		spread := bestAsk.Price - bestBid.Price

		flushedBook := &generated.OrderbookFlush{
			Exchange:        st.Exchange,
			Symbol:          st.Symbol,
			EventTimeMillis: st.TimestampMillis,
			Bids:            protoBids,
			Asks:            protoAsks,
			BestBid: &generated.OrderbookFlush_BookLevel{
				Price:  bestBid.Price,
				Volume: bestBid.Quantity,
			},
			BestAsk: &generated.OrderbookFlush_BookLevel{
				Price:  bestAsk.Price,
				Volume: bestAsk.Quantity,
			},
			Spread: spread,
		}

		keyBytes := []byte(key)
		valBytes, protoErr := proto.Marshal(flushedBook)
		if protoErr != nil {
			logger.Log.Info("Error when marshalling orderbook to flushed book", "exchange", st.Exchange, "symbol", st.Symbol, "worker", w.ID, "error", protoErr)
			continue
		}

		// flush to downstream
		kafka.ProduceAsync(w.ID, w.Ctx, kafka.Client, keyBytes, valBytes)
	}

	ackOffsetMap := make(map[int32]int64)

	// track the max offset which the worker has processed in all its tracked partitions
	// it will track x symbols in k partitions and a combination of them interleaved in multiple partitions
	// provide the max offset map to coordinator to show how far the worker has processed for his partitions

	for _, st := range w.OrderbookStateMap {
		for partition, offset := range st.LastProcessedOffset {
			ackOffsetMap[partition] = max(ackOffsetMap[partition], offset)
		}
	}

	logger.Log.Info("Sending ack to coordinator for worker, epoch", "epoch", flushEpoch, "worker", w.ID)

	// send the flush ack with epoch for distributed commit
	select {
	case w.AckChannel <- &constants.Ack{
		Epoch:            flushEpoch,
		WorkerID:         w.ID,
		PartitionOffsets: ackOffsetMap,
	}:
	default:
		logger.Log.Info("Dropping ack for worker to coordinator since ack channel is overloaded/blocked")
	}

	// distributed committer commits least offset for a symbol
	// will update the latest committed offset post commit at the kafka level
	// enqueues a snapshot event to worker channel post committing
}

func (w *Worker) HandleSnapshotRequest() {
	// this will clone the current orderbook state into another variable with snapshotOffset metadata
	// will post it to its snapshotchannel which is read by a goroutine

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

func (w *Worker) EvaluateAndDispatchSnapshot() {

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
			logger.Log.Warn("Snapshot offset > last committed offset. Cannot execute snapshot in this attempt", "worker", w.ID, "key", key)
			continue
		}

		logger.Log.Info("Snapshot execute condition success. Posting into snapshot channel for worker", "worker", w.ID, "key", key)
		w.SnapshotChannel <- &constants.SnapshotMsg{
			Snapshot: snapshot,
			Key:      key,
		}
	}
}

// to change the state only after successful redis write. once we know snapshot is persisted
func (w *Worker) HandleSnapshotPersisted(key string) {
	w.OrderbookStateMap[key].SnapshotPending = false
	delete(w.SnapshotStateMap, key)
}

func (w *Worker) SnapshotPersistLoop() {
	for {
		select {
		case <-w.Ctx.Done():
			logger.Log.Info("Received ctx done event.. returning worker snapshot execute loop")
			return
		case msg := <-w.SnapshotChannel:

			metrics.Orderbook_SnapshotRequestsTotal.WithLabelValues(strconv.Itoa(w.ID)).Inc()

			key := msg.Key
			snapshot := msg.Snapshot

			logger.Log.Info("Received message in snapshot channel", "worker", w.ID, "key", key)

			if len(snapshot.Bids) == 0 && len(snapshot.Asks) == 0 {
				metrics.Orderbook_EmptySnapshotsTotal.WithLabelValues(strconv.Itoa(w.ID)).Inc()
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
				metrics.Orderbook_SnapshotFailuresTotal.WithLabelValues(strconv.Itoa(w.ID)).Inc()
				continue
			}

			metrics.Orderbook_SnapshotSizeBytes.WithLabelValues(strconv.Itoa(w.ID)).Observe(float64(len(snapshotBytestream)))

			redisErr := redis.LoadSnapshot(w.Ctx, key, snapshotBytestream)
			if redisErr != nil {
				logger.Log.Error("Error in loading snapshot to redis", "key", key, "error", redisErr)
				metrics.Orderbook_SnapshotFailuresTotal.WithLabelValues(strconv.Itoa(w.ID)).Inc()
				continue
			}

			logger.Log.Info("Successfully loaded snapshot in redis for key", "worker", w.ID, "key", key)

			w.UpdateChannel <- &constants.DispatchRecord{
				Event:     constants.SnapshotPersistedEvent,
				BufferKey: key,
			}

			metrics.Orderbook_SnapshotSuccessesTotal.WithLabelValues(strconv.Itoa(w.ID)).Inc()
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
	if len(snapshotBytes) == 0 {
		logger.Log.Info("Stored snapshot not present. Creating new state", "worker", w.ID, "key", bufferKey)
		return &SymbolState{
			Exchange:            exchange,
			Symbol:              symbol,
			LastCommittedOffset: map[int32]int64{},
			LastProcessedOffset: map[int32]int64{},
			Restored:            false,
			Orderbook:           book.NewOrderBook(),
		}
	}

	logger.Log.Info("Retrieving stored snapshot and restoring state for key", "key", bufferKey)

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

// process the update ack from commit coordinator post offset commit and trigger snapshot execute
func (w *Worker) ProcessUpdateAck() {
	for {
		select {
		case <-w.Ctx.Done():
			logger.Log.Info("Received ctx done in process update ack. Returning", "worker", w.ID)
			return
		case updateAck := <-w.UpdateAckChannel:
			logger.Log.Info("Received ack from update ack channel post commit", "worker", w.ID, "epoch", updateAck.Epoch)
			for _, st := range w.OrderbookStateMap {
				copiedMap := make(map[int32]int64)
				for partition, offset := range updateAck.PartitionOffsets {
					copiedMap[partition] = offset
				}
				st.LastCommittedOffset = copiedMap
			}

			logger.Log.Info("Updated the last committed offsets for worker. Triggering snapshot execute", "worker", w.ID)

			// trigger snapshot execute post commit
			w.UpdateChannel <- &constants.DispatchRecord{
				Event: constants.SnapshotExecuteEvent,
			}
		}
	}
}

func (w *Worker) RunSnapshotPrepareScheduler() {
	ticker := time.NewTicker(time.Duration(w.SnapshotPrepareIntervalSeconds) * time.Second)
	for {
		select {
		case <-w.Ctx.Done():
			logger.Log.Info("Received ctx done in process update ack. Returning", "worker", w.ID)
			return
		case <-ticker.C:
			select {
			case w.UpdateChannel <- &constants.DispatchRecord{
				Event: constants.SnapshotRequestEvent,
			}:
			default:
				logger.Log.Info("Dropped handle snapshot request event as channel full", "worker", w.ID)
			}
		}
	}
}
