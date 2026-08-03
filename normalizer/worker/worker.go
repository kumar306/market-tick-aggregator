package worker

import (
	"context"
	"market-normalizer/constants"
	"market-normalizer/dedupe"
	"market-normalizer/factory/registry"
	"market-normalizer/kafka"
	"market-normalizer/proto/generated"
	"market-normalizer/utils"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
)

func ProcessRecord(ctx context.Context,
	dispatchRec *constants.DispatchRecord,
	workerMap map[string]*constants.SymbolState,
	workerChannel chan *constants.DispatchRecord,
	dedupeBuf []byte) error {

	// if key not in map - insert into the map and plug its strategies based on feed
	_, exists := workerMap[dispatchRec.BufferKey]
	if !exists {
		converter, err := registry.GetRegisteredConverter(dispatchRec.Exchange, dispatchRec.Channel)
		if err != nil {
			return logger.LogAndWrap("Error when fetching registered converter to worker", err)
		}

		orderer, err := registry.GetRegisteredOrderer(dispatchRec.Exchange, dispatchRec.Channel)
		if err != nil {
			return logger.LogAndWrap("Error when fetching registered orderer to worker", err)
		}

		normalizer, err := registry.GetRegisteredNormalizer(dispatchRec.Exchange, dispatchRec.Channel)
		if err != nil {
			return logger.LogAndWrap("Error when fetching registered normalizer to worker", err)
		}

		publisher, err := registry.GetRegisteredPublisher(dispatchRec.Channel)
		if err != nil {
			return logger.LogAndWrap("Error when fetching registered publisher to worker", err)
		}

		// creating the symbol state per bufferkey on init insertion
		workerMap[dispatchRec.BufferKey] = &constants.SymbolState{
			Orderer:    orderer,
			Converter:  converter,
			Normalizer: normalizer,
			Publisher:  publisher,
		}

		logger.Log.Info("Inserted entry for key in worker", "id", dispatchRec.ShardKey, "bufferKey", dispatchRec.BufferKey)
	}

	symbolState := workerMap[dispatchRec.BufferKey]

	// conversion
	normalizedMsg, err := symbolState.Converter.Convert(dispatchRec.Record.Value)
	if err != nil {
		return logger.LogAndWrap("Error in worker converter stage", err, "exchange", dispatchRec.Exchange, "channel", dispatchRec.Channel)
	}

	// worker insertion scenario
	if !exists {
		symbolState.Orderer.SetSymbolState(symbolState)
		symbolState.Orderer.InitOrdererState(normalizedMsg)
		logger.Log.Info("Set symbol state for key", "bufferKey", dispatchRec.BufferKey)
		exists = true
	}

	dedupeStartTime := time.Now()

	// dedupe check
	dedupeKey := dedupe.ConstructDedupeKeyInto(dedupeBuf,
		dispatchRec.Record.Topic,
		dispatchRec.Record.Partition,
		dispatchRec.Record.Offset)

	dedupeExists, err := dedupe.IsDuplicate(ctx, dedupeKey)
	if err != nil {
		metrics.Normalizer_DedupeErrorsTotal.WithLabelValues(dispatchRec.Exchange, dispatchRec.Channel, dispatchRec.Symbol).Inc()
		return logger.LogAndWrap("Error in worker dedupe check", err, "key", dedupeKey)
	}

	metrics.Normalizer_DedupeChecksTotal.WithLabelValues(dispatchRec.Exchange, dispatchRec.Channel, dispatchRec.Symbol).Inc()

	if dedupeExists {
		metrics.Normalizer_DedupeHitsTotal.WithLabelValues(dispatchRec.Exchange, dispatchRec.Channel, dispatchRec.Symbol).Inc()
		logger.Log.Warn("Duplicate message detected. Skipping", "key", dedupeKey)
		return nil
	}

	dedupeLatency := time.Since(dedupeStartTime).Seconds()
	metrics.Normalizer_DedupeLatencySeconds.WithLabelValues(
		dispatchRec.Exchange,
		dispatchRec.Channel,
		dispatchRec.Symbol).Observe(dedupeLatency)

	// include the original record so it can be marked for commit in the publisher
	normalizedMsg.Record = dispatchRec.Record

	normalizedBuf, err := symbolState.Orderer.Order(normalizedMsg, dispatchRec.BufferKey, workerChannel)

	if len(normalizedBuf) == 0 {
		// message added in the buffer case
		metrics.Normalizer_BufferSize.WithLabelValues(dispatchRec.Exchange,
			dispatchRec.Channel,
			dispatchRec.Symbol).Inc()
		return nil
	}

	// convert to a normalized schema and publish to downstream
	ProcessBuffer(ctx, normalizedBuf, dispatchRec.BufferKey, symbolState.Normalizer, symbolState.Publisher, symbolState.Orderer, dedupeBuf)
	return err
}

func ProcessBuffer(ctx context.Context,
	normalizedBuffer []*constants.PipelineMessage,
	partitionKey string,
	normalizer constants.NormalizerStrategy,
	publisher constants.PublisherStrategy,
	orderer constants.OrdererStrategy,
	dedupeBuf []byte) {

	for _, msg := range normalizedBuffer {
		protoStream, err := normalizer.Normalize(msg)
		if err != nil {
			metrics.Normalizer_NormalizedMessageErrorsTotal.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Inc()
			logger.Log.Error(err.Error())
			continue
		}

		metrics.Normalizer_NormalizedMessagesTotal.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Inc()

		publisher.Publish(ctx, protoStream, partitionKey, msg)

		// ack and update symbol state - by update strategy of orderer
		// if worker crashes mid flush, it will resume from crash point
		orderer.Ack(msg)

		// dec count after ack as map entry is deleted
		metrics.Normalizer_BufferSize.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Dec()

		// mark for dedupe
		dedupeErr := dedupe.MarkForDedupe(ctx, dedupe.ConstructDedupeKeyInto(dedupeBuf, msg.Record.Topic, msg.Record.Partition, msg.Record.Offset))

		if dedupeErr != nil {
			metrics.Normalizer_DedupeStoreErrorsTotal.WithLabelValues(msg.Exchange,
				msg.Channel,
				msg.Symbol).Inc()
		}
	}

	// final buffer internals cleanup
	orderer.Cleanup()
}

func FlushBuffer(ctx context.Context, dispatchRec *constants.DispatchRecord, workerMap map[string]*constants.SymbolState, dedupeBuf []byte) {
	symbolState := workerMap[dispatchRec.BufferKey]

	// process buffermap in order of increasing seq/timestamp
	// sort should happen based on orderer strategy
	sortedBuffer := symbolState.Orderer.PrepareBufferFlush()

	ProcessBuffer(ctx, sortedBuffer, dispatchRec.BufferKey, symbolState.Normalizer, symbolState.Publisher, symbolState.Orderer, dedupeBuf)
}

func ProcessSnapshotReady(ctx context.Context,
	dispatchRec *constants.DispatchRecord,
	workerMap map[string]*constants.SymbolState,
	workerChannel chan *constants.DispatchRecord,
	dedupeBuf []byte) {

	symbolState, ok := workerMap[dispatchRec.BufferKey]
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

	// check the first survivor and snapshot gap
	if len(survivors) > 0 {
		first := survivors[0].RawMessage.(*constants.BinanceDepthUpdateMsg)
		if first.FirstUpdateID > snapshot.LastUpdateID+1 {
			// gap between the snapshot and the buffered stream - snapshot is too stale. refetch
			logger.Log.Warn("Binance snapshot stale relative to buffered stream, refetching",
				"bufferKey", dispatchRec.BufferKey,
				"snapshotLastUpdateId", snapshot.LastUpdateID,
				"firstBufferedU", first.FirstUpdateID)
			symbolState.SnapshotPending = true
			go utils.FetchBinanceSnapshotAsync(dispatchRec.Exchange, dispatchRec.Channel, dispatchRec.Symbol, dispatchRec.BufferKey, workerChannel)
			return
		}
	}

	if snapshotProto, err := buildBinanceSnapshotProto(dispatchRec.Exchange, dispatchRec.Channel, dispatchRec.Symbol, snapshot); err != nil {
		logger.Log.Error("Failed to marshal binance snapshot proto", "err", err)
	} else {
		kafka.ProduceSnapshotAsync(ctx, symbolState.Publisher.PublishTopic(), []byte(dispatchRec.BufferKey), snapshotProto)
	}

	symbolState.NeedsSnapshot = false
	symbolState.LastSeqId = snapshot.LastUpdateID

	if len(survivors) > 0 {
		ProcessBuffer(ctx, survivors, dispatchRec.BufferKey, symbolState.Normalizer, symbolState.Publisher, symbolState.Orderer, dedupeBuf)
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
