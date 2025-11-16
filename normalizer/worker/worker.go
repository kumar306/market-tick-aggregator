package worker

import (
	"context"
	"market-normalizer/constants"
	"market-normalizer/factory"
	"shared/logger"
)

func ProcessRecord(ctx context.Context, dispatchRec *constants.DispatchRecord, workerMap map[string]*constants.SymbolState, workerChannel chan *constants.DispatchRecord) error {
	var err error

	// todo the pipeline logic:
	// redis dedupe check - do this at the last

	// if key not in map - insert into the map and plug its strategies based on feed
	_, exists := workerMap[dispatchRec.BufferKey]
	if !exists {
		// no need seq id backup here, i would go with current seq id - 1 as lastseqid for new worker map entry
		converter, err := factory.GetRegisteredConverter(dispatchRec.Exchange, dispatchRec.Channel)
		if err != nil {
			return logger.LogAndWrap("Error when fetching registered converter to worker", err)
		}

		orderer, err := factory.GetRegisteredOrderer(dispatchRec.Exchange, dispatchRec.Channel)
		if err != nil {
			return logger.LogAndWrap("Error when fetching registered orderer to worker", err)
		}

		normalizer, err := factory.GetRegisteredNormalizer(dispatchRec.Exchange, dispatchRec.Channel)
		if err != nil {
			return logger.LogAndWrap("Error when fetching registered normalizer to worker", err)
		}

		publisher, err := factory.GetRegisteredPublisher(dispatchRec.Channel)
		if err != nil {
			return logger.LogAndWrap("Error when fetching registered publisher to worker", err)
		}

		// have a method in orderer to init the orderer state
		workerMap[dispatchRec.BufferKey] = &constants.SymbolState{
			// creating the symbol state
			// LastSeqId: dispatchRec.Rec. current sequence id - this is fetched under different keys in binance, coinbase, kraken
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
		// log the error
	}

	if !exists {
		// worker insertion scenario
		symbolState.Orderer.InitOrdererState(normalizedMsg)
		exists = true
	}

	normalizedBuf, err := symbolState.Orderer.Order(normalizedMsg, dispatchRec.BufferKey, workerChannel)

	if len(normalizedBuf) == 0 {
		logger.Log.Info("Inserted in buffer. Returning")
		return nil
	}

	// convert to a normalized schema and publish to downstream
	ProcessBuffer(normalizedBuf, dispatchRec.BufferKey, symbolState.Normalizer, symbolState.Publisher, symbolState.Orderer)

	return err
}

func ProcessBuffer(normalizedBuffer []*constants.PipelineMessage, partitionKey string,
	normalizer constants.NormalizerStrategy,
	publisher constants.PublisherStrategy,
	orderer constants.OrdererStrategy) {

	for _, msg := range normalizedBuffer {

		protoStream, err := normalizer.Normalize(msg)
		if err != nil {
			// log the normalizer error
		}

		publisher.Publish(protoStream, []byte(partitionKey), msg.Exchange, msg.Channel)

		// ack and update symbol state - by update strategy of orderer
		// if worker crashes mid flush, it will resume from crash point
		orderer.Ack(msg)
	}

	// final buffer internals cleanup
	orderer.Cleanup()
}

func FlushBuffer(ctx context.Context, dispatchRec *constants.DispatchRecord, workerMap map[string]*constants.SymbolState) {
	symbolState, _ := workerMap[dispatchRec.BufferKey]

	// process buffermap in order of increasing seq/timestamp
	// sort should happen based on orderer strategy
	sortedBuffer := symbolState.Orderer.PrepareBufferFlush()

	ProcessBuffer(sortedBuffer, dispatchRec.BufferKey, symbolState.Normalizer, symbolState.Publisher, symbolState.Orderer)
}
