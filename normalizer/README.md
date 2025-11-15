// creating the symbol state. at init: just the pluggable objects are configured.
	// first need to determine what kind of orderer
	// getOrderer(exchange, channel) -> returns Orderer
	// BinanceAggTradeOrderer extends TimeOrderer
	// BinanceAggTradeOrderer (its keys to retrieve time - time orderer) - E
	// BinanceL2Orderer (diff keys to retrieve seq number - we fetch u so its seq order) - u
	// CoinbaseTickerOrderer (seq number present - sequence), CoinbaseL2Orderer (timestamp - time field of l2 update)
	// KrakenTickerOrderer - no time field present, KrakenBookOrderer - timestamp field present
	// Each feed, channel has time/sequence ordering and its ways to extract the sequence number/timestamp

	// convert to raw struct - get the raw struct converter strategy - and get the seq out. set symbol state set the last seq id as well
	// DETERMINE WHAT ordering to use: getConverter(exchange, channel)
	// BinanceAggTradeConverter, BinanceL2Converter, CoinbaseTickerConverter, CoinbaseDepthConverter, KrakenTickerConverter, KrakenBookConverter
	// one of the above is returned

	// the orderer strategy. start the deadline if dropped. update lastseqID to current processed seqID
	// orderer - sequenceorderer (coinbase ticker+book, binance book) / timestamporderer (binance ticker, kraken book) / nooporderer (kraken ticker has no time field)
	// whatever is returned from orderer strategy - []T
	// its passed into protobuf normalizer strategy -
	// passed into publisher strategy (normalized_tickers vs normalized_l2)