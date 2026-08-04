package worker

import (
	"context"
	"encoding/json"
	"market-normalizer/backpressure"
	"market-normalizer/constants"
	"market-normalizer/factory/registry"
	"market-normalizer/kafka"
	"market-normalizer/proto/generated"
	"market-normalizer/utils"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type Worker struct {
	ID           int
	EventChannel chan *constants.DispatchRecord
	WorkerMap    map[string]*constants.SymbolState
	Backpressure *backpressure.Controller
	txnFailed    atomic.Bool
}

func NewWorker(id int, eventCh chan *constants.DispatchRecord, bp *backpressure.Controller) *Worker {
	return &Worker{
		ID:           id,
		EventChannel: eventCh,
		WorkerMap:    make(map[string]*constants.SymbolState),
		Backpressure: bp,
	}
}

// Run owns the worker's whole transaction lifecycle. Consuming, buffer
// flushes (gap timers), Binance snapshot results, and the commit-interval
// boundary are all serialized through this one select loop -- exactly one
// goroutine ever touches this session.
func (w *Worker) Run(ctx context.Context, session *kgo.GroupTransactSession, commitInterval time.Duration) {
	defer session.Close()

	if err := session.Begin(); err != nil {
		logger.Log.Error("Failed to begin initial transaction", "worker", w.ID, "err", err)
		return
	}

	commitTicker := time.NewTicker(commitInterval)
	defer commitTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.endTransaction(context.Background(), session)
			return

		case dispatchRec, ok := <-w.EventChannel:
			if !ok {
				return
			}
			switch dispatchRec.Event {
			case constants.FlushBuffer:
				// posted from orderer
				w.FlushBuffer(ctx, dispatchRec, session)
			case constants.SnapshotReady:
				// posted after fetching rest snapshot
				w.ProcessSnapshotReady(ctx, dispatchRec, session)
			}

		case <-commitTicker.C:
			w.endTransaction(ctx, session)
			if err := session.Begin(); err != nil {
				logger.Log.Error("Failed to begin next transaction", "worker", w.ID, "err", err)
				return
			}

		default:
			pollCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			fetches := session.PollFetches(pollCtx)
			cancel()

			if fetches.IsClientClosed() {
				return
			}

			fetches.EachRecord(func(rec *kgo.Record) {
				if w.Backpressure != nil {
					w.Backpressure.OnEnqueue(rec.Topic, rec.Partition)
				}
				if err := w.ProcessRecord(ctx, rec, session); err != nil {
					logger.Log.Error("Error processing record", "worker", w.ID, "err", err)
				}
				if w.Backpressure != nil {
					w.Backpressure.OnDequeue()
				}
			})

			fetches.EachError(func(topic string, partition int32, err error) {
				logger.Log.Error("Fetch error", "worker", w.ID, "topic", topic, "partition", partition, "err", err)
			})
		}
	}
}

// TxnFailed reports whether the current cycle has a produce failure pending
// abort. Exposed for tests; production code shouldn't need to read this.
func (w *Worker) TxnFailed() bool {
	return w.txnFailed.Load()
}

func (w *Worker) endTransaction(ctx context.Context, session *kgo.GroupTransactSession) {
	commit := kgo.TryCommit
	if w.txnFailed.Swap(false) {
		commit = kgo.TryAbort
		logger.Log.Warn("Aborting transaction due to a produce failure this cycle", "worker", w.ID)
	}
	if _, err := session.End(ctx, commit); err != nil {
		logger.Log.Error("Transaction end failed", "worker", w.ID, "err", err)
	}
}

func (w *Worker) ProcessRecord(ctx context.Context, rec *kgo.Record, producer constants.Producer) error {
	var header constants.Header
	if err := json.Unmarshal(rec.Value, &header); err != nil {
		logger.Log.Error("Error in unmarshalling record header fields", "error", err)
		return err
	}

	symbol := string(rec.Key)
	bufferKey := strings.ToLower(header.Exchange) + ":" + strings.ToLower(header.Channel) + ":" + strings.ToLower(symbol)

	_, exists := w.WorkerMap[bufferKey]
	if !exists {
		converter, err := registry.GetRegisteredConverter(header.Exchange, header.Channel)
		if err != nil {
			return logger.LogAndWrap("Error when fetching registered converter to worker", err)
		}
		orderer, err := registry.GetRegisteredOrderer(header.Exchange, header.Channel)
		if err != nil {
			return logger.LogAndWrap("Error when fetching registered orderer to worker", err)
		}
		normalizer, err := registry.GetRegisteredNormalizer(header.Exchange, header.Channel)
		if err != nil {
			return logger.LogAndWrap("Error when fetching registered normalizer to worker", err)
		}
		publisher, err := registry.GetRegisteredPublisher(header.Channel)
		if err != nil {
			return logger.LogAndWrap("Error when fetching registered publisher to worker", err)
		}

		w.WorkerMap[bufferKey] = &constants.SymbolState{
			Orderer:    orderer,
			Converter:  converter,
			Normalizer: normalizer,
			Publisher:  publisher,
		}

		logger.Log.Info("Inserted entry for key in worker", "id", w.ID, "bufferKey", bufferKey)
	}

	symbolState := w.WorkerMap[bufferKey]

	normalizedMsg, err := symbolState.Converter.Convert(rec.Value)
	if err != nil {
		return logger.LogAndWrap("Error in worker converter stage", err, "exchange", header.Exchange, "channel", header.Channel)
	}

	if !exists {
		symbolState.Orderer.SetSymbolState(symbolState)
		symbolState.Orderer.InitOrdererState(normalizedMsg)
		logger.Log.Info("Set symbol state for key", "bufferKey", bufferKey)
	}

	// carried through for the drop-marker check and resync signalling
	normalizedMsg.Record = rec

	normalizedBuf, err := symbolState.Orderer.Order(normalizedMsg, bufferKey, w.EventChannel)
	if len(normalizedBuf) == 0 {
		metrics.Normalizer_BufferSize.WithLabelValues(header.Exchange, header.Channel, symbol).Inc()
		return nil
	}

	onFailure := func() { w.txnFailed.Store(true) }
	ProcessBuffer(ctx, normalizedBuf, bufferKey, symbolState.Normalizer, symbolState.Publisher, symbolState.Orderer, producer, onFailure)
	return err
}

func ProcessBuffer(ctx context.Context,
	normalizedBuffer []*constants.PipelineMessage,
	partitionKey string,
	normalizer constants.NormalizerStrategy,
	publisher constants.PublisherStrategy,
	orderer constants.OrdererStrategy,
	producer constants.Producer,
	onFailure func()) {

	for _, msg := range normalizedBuffer {
		protoStream, err := normalizer.Normalize(msg)
		if err != nil {
			metrics.Normalizer_NormalizedMessageErrorsTotal.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Inc()
			logger.Log.Error(err.Error())
			continue
		}

		metrics.Normalizer_NormalizedMessagesTotal.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Inc()

		publisher.Publish(ctx, protoStream, partitionKey, msg, producer, onFailure)

		orderer.Ack(msg)

		metrics.Normalizer_BufferSize.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Dec()
	}

	orderer.Cleanup()
}

func (w *Worker) FlushBuffer(ctx context.Context, dispatchRec *constants.DispatchRecord, producer constants.Producer) {
	symbolState := w.WorkerMap[dispatchRec.BufferKey]

	sortedBuffer := symbolState.Orderer.PrepareBufferFlush()

	onFailure := func() { w.txnFailed.Store(true) }
	ProcessBuffer(ctx, sortedBuffer, dispatchRec.BufferKey, symbolState.Normalizer, symbolState.Publisher, symbolState.Orderer, producer, onFailure)
}

func (w *Worker) ProcessSnapshotReady(ctx context.Context, dispatchRec *constants.DispatchRecord, producer constants.Producer) {
	symbolState, ok := w.WorkerMap[dispatchRec.BufferKey]
	if !ok {
		return
	}

	symbolState.SnapshotPending = false

	if dispatchRec.SnapshotResult == nil {
		logger.Log.Warn("Binance snapshot fetch failed, will retry on next message", "bufferKey", dispatchRec.BufferKey)
		return
	}

	snapshot := dispatchRec.SnapshotResult
	sortedBuffer := symbolState.Orderer.PrepareBufferFlush()

	survivors := sortedBuffer[:0]
	for _, m := range sortedBuffer {
		depthMsg, ok := m.RawMessage.(*constants.BinanceDepthUpdateMsg)
		if !ok || depthMsg.FinalUpdateID <= snapshot.LastUpdateID {
			continue
		}
		survivors = append(survivors, m)
	}

	if len(survivors) > 0 {
		first := survivors[0].RawMessage.(*constants.BinanceDepthUpdateMsg)
		if first.FirstUpdateID > snapshot.LastUpdateID+1 {
			logger.Log.Warn("Binance snapshot stale relative to buffered stream, refetching",
				"bufferKey", dispatchRec.BufferKey,
				"snapshotLastUpdateId", snapshot.LastUpdateID,
				"firstBufferedU", first.FirstUpdateID)
			symbolState.SnapshotPending = true
			go utils.FetchBinanceSnapshotAsync(dispatchRec.Exchange, dispatchRec.Channel, dispatchRec.Symbol, dispatchRec.BufferKey, w.EventChannel)
			return
		}
	}

	if snapshotProto, err := buildBinanceSnapshotProto(dispatchRec.Exchange, dispatchRec.Channel, dispatchRec.Symbol, snapshot); err != nil {
		logger.Log.Error("Failed to marshal binance snapshot proto", "err", err)
	} else {
		kafka.ProduceSnapshotAsync(producer, symbolState.Publisher.PublishTopic(), []byte(dispatchRec.BufferKey), snapshotProto)
	}

	symbolState.NeedsSnapshot = false
	symbolState.LastSeqId = snapshot.LastUpdateID

	if len(survivors) > 0 {
		onFailure := func() { w.txnFailed.Store(true) }
		ProcessBuffer(ctx, survivors, dispatchRec.BufferKey, symbolState.Normalizer, symbolState.Publisher, symbolState.Orderer, producer, onFailure)
	}

	symbolState.Orderer.Cleanup()
}

func buildBinanceSnapshotProto(exchange, channel, symbol string, snapshot *constants.BinanceDepthSnapshot) ([]byte, error) {
	normalizedMsg := generated.NormalizedBook{
		Exchange:        exchange,
		Channel:         channel,
		Symbol:          symbol,
		EventType:       constants.Snapshot,
		EventTimeMillis: time.Now().UnixMilli(),
		Bids:            make([]*generated.NormalizedBook_BookLevel, 0, len(snapshot.Bids)),
		Asks:            make([]*generated.NormalizedBook_BookLevel, 0, len(snapshot.Asks)),
	}

	for _, bid := range snapshot.Bids {
		price, _ := strconv.ParseFloat(bid[0], 64)
		volume, _ := strconv.ParseFloat(bid[1], 64)
		normalizedMsg.Bids = append(normalizedMsg.Bids, &generated.NormalizedBook_BookLevel{Price: price, Volume: volume})
	}
	for _, ask := range snapshot.Asks {
		price, _ := strconv.ParseFloat(ask[0], 64)
		volume, _ := strconv.ParseFloat(ask[1], 64)
		normalizedMsg.Asks = append(normalizedMsg.Asks, &generated.NormalizedBook_BookLevel{Price: price, Volume: volume})
	}

	return proto.Marshal(&normalizedMsg)
}
