package writer

import (
	"context"
	"market-persistence/batcher/util"
	"market-persistence/db/model"
	"shared/logger"
	"shared/metrics"
)

func FlushOrderbook(ctx context.Context, tx util.Tx, rows []*model.OrderbookFlush) error {
	const flushSql = `
		INSERT INTO orderbook_flushes(
		exchange, symbol, 
		event_time_millis, event_time,
		best_bid_price, best_bid_volume,
		best_ask_price, best_ask_volume,
		spread
		)
		VALUES (
		$1, $2,
		$3, $4,
		$5, $6,
		$7, $8,
		$9
		) ON CONFLICT (exchange, symbol, event_time) DO NOTHING;
	`

	const levelSql = `
		INSERT INTO orderbook_flush_levels(
			exchange, symbol, event_time, 
			level_index, side,
			price, volume
		) VALUES (
			$1, $2, $3,
			$4, $5,
			$6, $7 
		) ON CONFLICT (exchange, symbol, event_time, side, level_index) DO NOTHING;
	`
	parentRowsAffected := 0
	levelRowsAffected := 0

	for _, row := range rows {

		// insert parent row then insert level rows
		r, err := tx.Exec(ctx,
			flushSql,
			row.FlushRow.Exchange, row.FlushRow.Symbol,
			row.FlushRow.EventTimeMillis, row.FlushRow.EventTime,
			row.FlushRow.BestBidPrice, row.FlushRow.BestBidVolume,
			row.FlushRow.BestAskPrice, row.FlushRow.BestAskVolume,
			row.FlushRow.Spread,
		)
		if err != nil {
			logger.Log.Error("Error in inserting into orderbook_flushes", "Error", err)
			return err
		}
		parentRowsAffected += int(r)

		for _, levelRow := range row.LevelRows {
			lr, err := tx.Exec(ctx,
				levelSql,
				row.FlushRow.Exchange, row.FlushRow.Symbol,
				row.FlushRow.EventTime,
				levelRow.LevelIndex, levelRow.Side,
				levelRow.Price, levelRow.Volume)
			if err != nil {
				logger.Log.Error("Error in inserting into orderbook_flush_levels", "Error", err)
				return err
			}

			levelRowsAffected += int(lr)
		}
	}

	logger.Log.Info("Total rows affected: ", "totalRows", parentRowsAffected+levelRowsAffected, "orderbook_flushes", parentRowsAffected, "orderbook_flush_levels", levelRowsAffected)
	metrics.Persistence_DbRowsWritten.WithLabelValues("orderbook_flushes").Add(float64(parentRowsAffected))
	metrics.Persistence_DbRowsWritten.WithLabelValues("orderbook_flush_levels").Add(float64(levelRowsAffected))

	return nil
}
