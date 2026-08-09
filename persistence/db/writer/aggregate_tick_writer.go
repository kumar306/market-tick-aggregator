package writer

import (
	"context"
	"market-persistence/batcher/util"
	"market-persistence/db/model"
	"shared/logger"
	"shared/metrics"
	"time"
)

const stagingAggregatedTicksDDL = `
	CREATE TEMP TABLE IF NOT EXISTS staging_aggregated_ticks (
		exchange TEXT NOT NULL,
		symbol TEXT NOT NULL,
		window_id TEXT NOT NULL,
		start_ts_ms BIGINT NOT NULL,
		end_ts_ms BIGINT NOT NULL,
		start_ts TIMESTAMPTZ NOT NULL,
		end_ts TIMESTAMPTZ NOT NULL,
		open_price DOUBLE PRECISION NOT NULL,
		close_price DOUBLE PRECISION NOT NULL,
		low_price DOUBLE PRECISION NOT NULL,
		high_price DOUBLE PRECISION NOT NULL,
		vwap DOUBLE PRECISION NOT NULL,
		rolling_vwap DOUBLE PRECISION NOT NULL,
		twap DOUBLE PRECISION NOT NULL,
		microprice DOUBLE PRECISION NOT NULL,
		volume DOUBLE PRECISION NOT NULL,
		rolling_volume DOUBLE PRECISION NOT NULL,
		volume_acceleration DOUBLE PRECISION NOT NULL,
		volatility DOUBLE PRECISION NOT NULL,
		atr DOUBLE PRECISION NOT NULL,
		ema DOUBLE PRECISION NOT NULL,
		sma DOUBLE PRECISION NOT NULL,
		log_return DOUBLE PRECISION NOT NULL,
		simple_return DOUBLE PRECISION NOT NULL
	) ON COMMIT DROP;
	`

// INSERT SELECT into the partitioned parent table routes each row to the correct partition automatically
const insertFromStagingSql = `
	INSERT INTO aggregated_ticks(
		exchange, symbol, window_id,
		start_ts_ms, end_ts_ms,
		start_ts, end_ts,
		open_price, close_price, low_price, high_price,
		vwap, rolling_vwap, twap, microprice,
		volume, rolling_volume, volume_acceleration,
		volatility, atr,
		ema, sma, log_return, simple_return
	)
	SELECT
		exchange, symbol, window_id,
		start_ts_ms, end_ts_ms,
		start_ts, end_ts,
		open_price, close_price, low_price, high_price,
		vwap, rolling_vwap, twap, microprice,
		volume, rolling_volume, volume_acceleration,
		volatility, atr,
		ema, sma, log_return, simple_return
	FROM staging_aggregated_ticks
	ON CONFLICT (exchange, symbol, window_id, start_ts) DO NOTHING;
`

var aggregatedTickColumns = []string{
	"exchange", "symbol", "window_id",
	"start_ts_ms", "end_ts_ms",
	"start_ts", "end_ts",
	"open_price", "close_price", "low_price", "high_price",
	"vwap", "rolling_vwap", "twap", "microprice",
	"volume", "rolling_volume", "volume_acceleration",
	"volatility", "atr",
	"ema", "sma", "log_return", "simple_return",
}

// aggregatedTickRowSource adapts []*model.AggregatedTick to pgx.copyFromSource,
// so the whole batch loads via one copy instead of one round trip per row.
type aggregatedTickRowSource struct {
	rows []*model.AggregatedTick
	idx  int
}

func (s *aggregatedTickRowSource) Next() bool {
	s.idx++
	return s.idx <= len(s.rows)
}

func (s *aggregatedTickRowSource) Values() ([]any, error) {
	row := s.rows[s.idx-1]
	return []any{
		row.Exchange, row.Symbol, row.WindowId,
		row.StartTsMs, row.EndTsMs,
		row.StartTs, row.EndTs,
		row.Open, row.Close, row.Low, row.High,
		row.VWAP, row.RollingVWAP, row.TWAP, row.Microprice,
		row.Volume, row.RollingVolume, row.VolumeAcceleration,
		row.Volatility, row.Atr,
		row.Ema, row.Sma, row.LogReturn, row.SimpleReturn,
	}, nil
}

func (s *aggregatedTickRowSource) Err() error {
	return nil
}

// use copy-from over insert to bulk insert into db rather than exec per row
func FlushAggregateTicks(ctx context.Context, tx util.Tx, rows []*model.AggregatedTick) error {
	if _, err := tx.Exec(ctx, stagingAggregatedTicksDDL); err != nil {
		logger.Log.Error("Error creating staging table for aggregated_ticks", "error", err)
		return err
	}

	copied, err := tx.CopyFrom(ctx, "staging_aggregated_ticks", aggregatedTickColumns, &aggregatedTickRowSource{rows: rows})
	if err != nil {
		logger.Log.Error("Error in COPY into staging_aggregated_ticks", "error", err)
		return err
	}

	rowsAffected, err := tx.Exec(ctx, insertFromStagingSql)
	if err != nil {
		logger.Log.Error("Error inserting from staging into aggregated_ticks", "error", err)
		return err
	}

	logger.Log.Info("Rows affected", "copied", copied, "inserted", rowsAffected)
	metrics.Persistence_DbRowsWritten.WithLabelValues("aggregated_ticks").Add(float64(rowsAffected))

	now := time.Now()
	for _, row := range rows {
		metrics.Persistence_TickFlushLagSeconds.Observe(now.Sub(row.EndTs).Seconds())
	}

	return nil
}
