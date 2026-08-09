package writer

import (
	"context"
	"market-persistence/batcher/util"
	"market-persistence/db/model"
	"shared/logger"
	"shared/metrics"
	"time"
)

const stagingOrderbookFlushesDDL = `
	CREATE TEMP TABLE IF NOT EXISTS staging_orderbook_flushes (
		exchange TEXT NOT NULL,
		symbol TEXT NOT NULL,
		event_time_millis BIGINT NOT NULL,
		event_time TIMESTAMPTZ NOT NULL,
		best_bid_price DOUBLE PRECISION NOT NULL,
		best_bid_volume DOUBLE PRECISION NOT NULL,
		best_ask_price DOUBLE PRECISION NOT NULL,
		best_ask_volume DOUBLE PRECISION NOT NULL,
		spread DOUBLE PRECISION NOT NULL
	) ON COMMIT DROP;
	`

const stagingOrderbookFlushLevelsDDL = `
	CREATE TEMP TABLE IF NOT EXISTS staging_orderbook_flush_levels (
		exchange TEXT NOT NULL,
		symbol TEXT NOT NULL,
		event_time TIMESTAMPTZ NOT NULL,
		level_index INT NOT NULL,
		side TEXT NOT NULL,
		price DOUBLE PRECISION NOT NULL,
		volume DOUBLE PRECISION NOT NULL
	) ON COMMIT DROP;
	`

const insertOrderbookFlushesFromStagingSql = `
	INSERT INTO orderbook_flushes(
		exchange, symbol,
		event_time_millis, event_time,
		best_bid_price, best_bid_volume,
		best_ask_price, best_ask_volume,
		spread
	)
	SELECT
		exchange, symbol,
		event_time_millis, event_time,
		best_bid_price, best_bid_volume,
		best_ask_price, best_ask_volume,
		spread
	FROM staging_orderbook_flushes
	ON CONFLICT (exchange, symbol, event_time) DO NOTHING;
`

const insertOrderbookFlushLevelsFromStagingSql = `
	INSERT INTO orderbook_flush_levels(
		exchange, symbol, event_time,
		level_index, side,
		price, volume
	)
	SELECT
		exchange, symbol, event_time,
		level_index, side,
		price, volume
	FROM staging_orderbook_flush_levels
	ON CONFLICT (exchange, symbol, event_time, side, level_index) DO NOTHING;
`

var orderbookFlushColumns = []string{
	"exchange", "symbol",
	"event_time_millis", "event_time",
	"best_bid_price", "best_bid_volume",
	"best_ask_price", "best_ask_volume",
	"spread",
}

var orderbookFlushLevelColumns = []string{
	"exchange", "symbol", "event_time",
	"level_index", "side",
	"price", "volume",
}

// orderbookFlushRowSource adapts []*model.OrderbookFlush's parent rows to
// pgx.CopyFromSource, one row per symbol's flush.
type orderbookFlushRowSource struct {
	rows []*model.OrderbookFlush
	idx  int
}

func (s *orderbookFlushRowSource) Next() bool {
	s.idx++
	return s.idx <= len(s.rows)
}

func (s *orderbookFlushRowSource) Values() ([]any, error) {
	row := s.rows[s.idx-1].FlushRow
	return []any{
		row.Exchange, row.Symbol,
		row.EventTimeMillis, row.EventTime,
		row.BestBidPrice, row.BestBidVolume,
		row.BestAskPrice, row.BestAskVolume,
		row.Spread,
	}, nil
}

func (s *orderbookFlushRowSource) Err() error {
	return nil
}

// flattens []*model.OrderbookFlush's nested level
// rows into a single pgx.CopyFromSource stream, carrying each level's parent
// exchange/symbol/event_time alongside it
type orderbookLevelRowSource struct {
	rows     []*model.OrderbookFlush
	outerIdx int
	innerIdx int
}

func newOrderbookLevelRowSource(rows []*model.OrderbookFlush) *orderbookLevelRowSource {
	return &orderbookLevelRowSource{rows: rows, innerIdx: -1}
}

func (s *orderbookLevelRowSource) Next() bool {
	for s.outerIdx < len(s.rows) {
		s.innerIdx++
		if s.innerIdx < len(s.rows[s.outerIdx].LevelRows) {
			return true
		}
		s.outerIdx++
		s.innerIdx = -1
	}
	return false
}

func (s *orderbookLevelRowSource) Values() ([]any, error) {
	parent := s.rows[s.outerIdx].FlushRow
	level := s.rows[s.outerIdx].LevelRows[s.innerIdx]
	return []any{
		parent.Exchange, parent.Symbol, parent.EventTime,
		level.LevelIndex, level.Side,
		level.Price, level.Volume,
	}, nil
}

func (s *orderbookLevelRowSource) Err() error {
	return nil
}

// bulk loads a batch of symbols' order book flushes via COPY
// + staging tables, instead of one Exec per parent row plus one Exec per level row
// same structure as aggregate tick writer - 2 staging table creation, 2 copy to staging table and 2 insert = 6 network round trip
// a batch of N symbols with L levels each, this replaces
// N*(1+L) round trips to Postgres with a fixed 6 (2 staging DDLs, 2 COPYs,
// 2 insert-from-staging execs), regardless of batch size
func FlushOrderbook(ctx context.Context, tx util.Tx, rows []*model.OrderbookFlush) error {
	if len(rows) == 0 {
		return nil
	}

	if _, err := tx.Exec(ctx, stagingOrderbookFlushesDDL); err != nil {
		logger.Log.Error("Error creating staging table for orderbook_flushes", "error", err)
		return err
	}
	if _, err := tx.Exec(ctx, stagingOrderbookFlushLevelsDDL); err != nil {
		logger.Log.Error("Error creating staging table for orderbook_flush_levels", "error", err)
		return err
	}

	copiedFlushes, err := tx.CopyFrom(ctx, "staging_orderbook_flushes", orderbookFlushColumns, &orderbookFlushRowSource{rows: rows})
	if err != nil {
		logger.Log.Error("Error in COPY into staging_orderbook_flushes", "error", err)
		return err
	}

	copiedLevels, err := tx.CopyFrom(ctx, "staging_orderbook_flush_levels", orderbookFlushLevelColumns, newOrderbookLevelRowSource(rows))
	if err != nil {
		logger.Log.Error("Error in COPY into staging_orderbook_flush_levels", "error", err)
		return err
	}

	parentRowsAffected, err := tx.Exec(ctx, insertOrderbookFlushesFromStagingSql)
	if err != nil {
		logger.Log.Error("Error inserting from staging into orderbook_flushes", "error", err)
		return err
	}

	levelRowsAffected, err := tx.Exec(ctx, insertOrderbookFlushLevelsFromStagingSql)
	if err != nil {
		logger.Log.Error("Error inserting from staging into orderbook_flush_levels", "error", err)
		return err
	}

	logger.Log.Info("Rows affected", "copiedFlushes", copiedFlushes, "copiedLevels", copiedLevels, "orderbook_flushes", parentRowsAffected, "orderbook_flush_levels", levelRowsAffected)
	metrics.Persistence_DbRowsWritten.WithLabelValues("orderbook_flushes").Add(float64(parentRowsAffected))
	metrics.Persistence_DbRowsWritten.WithLabelValues("orderbook_flush_levels").Add(float64(levelRowsAffected))

	now := time.Now()
	for _, row := range rows {
		metrics.Persistence_BookFlushLagSeconds.Observe(now.Sub(row.FlushRow.EventTime).Seconds())
	}

	return nil
}
