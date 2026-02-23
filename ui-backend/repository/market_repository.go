package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type MarketRepository struct {
	db *pgxpool.Pool
}

type CandleRow struct {
	Open    float64
	Close   float64
	Low     float64
	High    float64
	Volume  float64
	StartTs time.Time
	EndTs   time.Time
}

type MetricRow struct {
	Exchange           string
	Symbol             string
	WindowId           string
	StartTs            time.Time
	EndTs              time.Time
	VWAP               float64
	RollingVWAP        float64
	TWAP               float64
	Microprice         float64
	Volume             float64
	RollingVolume      float64
	VolumeAcceleration float64
	Volatility         float64
	Atr                float64
	Ema                float64
	Sma                float64
	LogReturn          float64
	SimpleReturn       float64
}

type OrderbookRow struct {
	Exchange      string
	Symbol        string
	EventTime     time.Time
	BestBidPrice  float64
	BestBidVolume float64
	BestAskPrice  float64
	BestAskVolume float64
	Spread        float64
	Levels        map[string][]*OrderbookLevelRow
}

type OrderbookLevelRow struct {
	LevelIndex int
	Price      float64
	Volume     float64
}

func NewMarketRepository(db *pgxpool.Pool) *MarketRepository {
	return &MarketRepository{db: db}
}

func (m *MarketRepository) GetCandles(ctx context.Context,
	exchange string,
	symbol string,
	from time.Time,
	to time.Time) ([]*CandleRow, error) {
	rows, err := m.db.Query(ctx, `
		SELECT start_ts, end_ts, open, close, low, high, volume 
		FROM aggregated_ticks
		WHERE exchange = $1 
		AND symbol = $2
		AND start_ts >= $3
		AND end_ts <= $4
		ORDER BY start_ts
		`, exchange, symbol, from, to)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var results []*CandleRow

	for rows.Next() {
		row := CandleRow{}
		err := rows.Scan(
			&row.StartTs,
			&row.EndTs,
			&row.Open,
			&row.Close,
			&row.Low,
			&row.High,
			&row.Volume,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, &row)
	}

	return results, nil
}

func (m *MarketRepository) GetMetrics(ctx context.Context,
	exchange string,
	symbol string,
	windows []string,
	metrics []string,
	from time.Time, to time.Time) ([]*MetricRow, error) {

	// select from aggregated ticks - all columns would be fetched.
	// then limit it at service layer
	rows, err := m.db.Query(ctx, `
		SELECT exchange, symbol, window_id, start_ts, end_ts, vwap, rolling_vwap, twap, microprice, volume, rolling_volume,
		volume_acceleration, volatility, atr, ema, sma, log_return, simple_return
		FROM aggregated_ticks
		WHERE exchange = $1
		AND symbol = $2
		AND window_id = ANY($3)
		AND start_ts >= $4
		AND end_ts <= $5 ORDER BY (end_ts, start_ts)
		`, exchange, symbol, windows, from, to)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*MetricRow

	for rows.Next() {
		row := MetricRow{}
		rows.Scan(
			&row.Exchange,
			&row.Symbol,
			&row.WindowId,
			&row.StartTs,
			&row.EndTs,
			&row.VWAP,
			&row.RollingVWAP,
			&row.TWAP,
			&row.Microprice,
			&row.Volume,
			&row.RollingVolume,
			&row.VolumeAcceleration,
			&row.Volatility,
			&row.Atr,
			&row.Ema,
			&row.Sma,
			&row.LogReturn,
			&row.SimpleReturn,
		)
		results = append(results, &row)
	}

	return results, nil
}

// api response : { exchange, symbol, best_bid, best_ask, spread, event time, levels: {"b": [{level, price, volume}], "s": []}}
func (m *MarketRepository) GetLatestBook(ctx context.Context, exchange, symbol string, depth int) (*OrderbookRow, error) {
	var result OrderbookRow

	row := m.db.QueryRow(ctx, `
	SELECT exchange, symbol, event_time,
	best_bid_price, best_bid_volume,
	best_ask_price, best_ask_volume,
	spread
	FROM orderbook_flushes
	WHERE exchange = $1 AND symbol = $2
	ORDER BY event_time DESC LIMIT 1
	`, exchange, symbol)

	err := row.Scan(&result.Exchange,
		&result.Symbol,
		&result.EventTime,
		&result.BestBidPrice,
		&result.BestBidVolume,
		&result.BestAskPrice,
		&result.BestAskVolume,
		&result.Spread)

	if err != nil {
		return nil, err
	}

	levelRows, err := m.db.Query(ctx, `
	SELECT side, level_index, price, volume
	FROM (
		SELECT
			side,
			level_index,
			price,
			volume,
			ROW_NUMBER() OVER (PARTITION BY side ORDER BY level_index) AS rn
		FROM orderbook_flush_levels
		WHERE exchange = $1 AND symbol = $2 AND event_time = $3
	) ranked
	WHERE rn <= $4
	ORDER BY side, level_index
	`, exchange, symbol, result.EventTime, depth)
	if err != nil {
		return nil, err
	}

	defer levelRows.Close()

	levelMap := map[string][]*OrderbookLevelRow{}

	for levelRows.Next() {
		var row OrderbookLevelRow
		var side string

		levelRows.Scan(&side, &row.LevelIndex, &row.Price, &row.Volume)

		if levelMap[side] == nil {
			levelMap[side] = make([]*OrderbookLevelRow, 0)
		}

		levelMap[side] = append(levelMap[side], &row)
	}

	result.Levels = levelMap

	return &result, nil
}
