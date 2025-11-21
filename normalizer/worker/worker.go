package worker

import (
	"context"
	"market-normalizer/constants"
	"market-normalizer/dedupe"
	"market-normalizer/factory/registry"
	"shared/logger"
	"shared/metrics"
	"time"
)

func ProcessRecord(ctx context.Context,
	dispatchRec *constants.DispatchRecord,
	workerMap map[string]*constants.SymbolState,
	workerChannel chan *constants.DispatchRecord) error {

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
		exists = true
	}

	dedupeStartTime := time.Now()

	// dedupe check
	dedupeKey := dedupe.ConstructDedupeKey(
		dispatchRec.Exchange,
		dispatchRec.Channel,
		dispatchRec.Symbol,
		symbolState.Orderer.GetOrderingId(normalizedMsg))

	dedupeExists, err := dedupe.IsDuplicate(ctx, dedupeKey)
	metrics.Normalizer_DedupeChecksTotal.WithLabelValues(dispatchRec.Exchange, dispatchRec.Channel, dispatchRec.Symbol).Inc()
	if err != nil {
		return logger.LogAndWrap("Error in worker dedupe check", err, "key", dedupeKey)
	}

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
		logger.Log.Info("Inserted in buffer. Returning")
		return nil
	}

	// convert to a normalized schema and publish to downstream
	ProcessBuffer(ctx, normalizedBuf, dispatchRec.BufferKey, symbolState.Normalizer, symbolState.Publisher, symbolState.Orderer)
	return err
}

func ProcessBuffer(ctx context.Context,
	normalizedBuffer []*constants.PipelineMessage,
	partitionKey string,
	normalizer constants.NormalizerStrategy,
	publisher constants.PublisherStrategy,
	orderer constants.OrdererStrategy) {

	for _, msg := range normalizedBuffer {

		protoStream, err := normalizer.Normalize(msg)
		if err != nil {
			metrics.Normalizer_NormalizedMessageErrorsTotal.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Inc()
			logger.Log.Error(err.Error())
			continue
		}

		metrics.Normalizer_NormalizedMessagesTotal.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Inc()

		publisher.Publish(protoStream, partitionKey, msg)

		// ack and update symbol state - by update strategy of orderer
		// if worker crashes mid flush, it will resume from crash point
		orderer.Ack(msg)

		// dec count after ack as map entry is deleted
		metrics.Normalizer_BufferSize.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Dec()

		// mark for dedupe
		dedupeErr := dedupe.MarkForDedupe(ctx, dedupe.ConstructDedupeKey(msg.Exchange,
			msg.Channel,
			msg.Symbol,
			orderer.GetOrderingId(msg)))

		if dedupeErr != nil {
			metrics.Normalizer_DedupeStoreErrorsTotal.WithLabelValues(msg.Exchange,
				msg.Channel,
				msg.Symbol).Inc()
		}
	}

	// final buffer internals cleanup
	orderer.Cleanup()
}

func FlushBuffer(ctx context.Context, dispatchRec *constants.DispatchRecord, workerMap map[string]*constants.SymbolState) {
	symbolState := workerMap[dispatchRec.BufferKey]

	// process buffermap in order of increasing seq/timestamp
	// sort should happen based on orderer strategy
	sortedBuffer := symbolState.Orderer.PrepareBufferFlush()

	ProcessBuffer(ctx, sortedBuffer, dispatchRec.BufferKey, symbolState.Normalizer, symbolState.Publisher, symbolState.Orderer)
}
