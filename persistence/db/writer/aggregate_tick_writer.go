package writer

import (
	"context"
	"market-persistence/batcher/util"
	"market-persistence/db/model"
	"shared/logger"
	"shared/metrics"
)

func FlushAggregateTicks(ctx context.Context, tx util.Tx, rows []*model.AggregatedTick) error {
	const sql = `
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
	VALUES (
	$1, $2, $3,
	$4, $5,
	$6, $7,
	$8, $9, $10, $11,
	$12, $13, $14, $15,
	$16, $17, $18,
	$19, $20,
	$21, $22, $23, $24
	) ON CONFLICT (exchange, symbol, window_id, start_ts)
	DO NOTHING; 
	`

	totalRowsAffected := 0
	for _, row := range rows {
		rowsAffected, err := tx.Exec(ctx,
			sql,
			row.Exchange, row.Symbol, row.WindowId,
			row.StartTsMs, row.EndTsMs,
			row.StartTs, row.EndTs,
			row.Open, row.Close, row.Low, row.High,
			row.VWAP, row.RollingVWAP, row.TWAP, row.Microprice,
			row.Volume, row.RollingVolume, row.VolumeAcceleration,
			row.Volatility, row.Atr,
			row.Ema, row.Sma, row.LogReturn, row.SimpleReturn)
		if err != nil {
			logger.Log.Error("Error in inserting into aggregated_ticks", "error", err)
			return err
		}
		totalRowsAffected += int(rowsAffected)
	}

	logger.Log.Info("Rows affected", "count", totalRowsAffected)
	metrics.Persistence_DbRowsWritten.WithLabelValues("aggregated_ticks").Add(float64(totalRowsAffected))
	return nil
}
