package worker

import (
	"context"
	"market-normalizer/constants"
)

func ProcessRecord(ctx context.Context, dispatchRec *constants.DispatchRecord, workerMap map[string]*constants.SymbolState) error {
	var err error

	// todo the pipeline logic:
	// redis dedupe check - do this at the last

	// if key not in map - insert into the map and plug its strategies based on feed
	if _, exists := workerMap[dispatchRec.BufferKey]; !exists {
		// no need seq id backup here, i would go with current seq id - 1 as lastseqid for new worker map entry
		workerMap[dispatchRec.BufferKey] = &constants.SymbolState{
			// creating the symbol state
			// LastSeqId: dispatchRec.Rec. current sequence id - this is fetched under different keys in binance, coinbase, kraken
			// raw struct converter - should take care of getting the seq id out and setting in symbol state
		}
	}

	// convert to raw struct - get the raw struct converter strategy - and get the seq out. set symbol state set the last seq id as well

	// the orderer strategy. start the deadline if dropped. update lastseqID to current processed seqID
	// orderer - sequenceorderer (coinbase ticker+book, binance book) / timestamporderer (binance ticker, kraken book) / nooporderer (kraken ticker has no time field)
	// whatever is returned from orderer strategy - []T
	// its passed into protobuf normalizer strategy -
	// passed into publisher strategy (normalized_tickers vs normalized_l2)

	return err
}
