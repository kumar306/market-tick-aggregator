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
