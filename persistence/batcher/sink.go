package batcher

import (
	"context"
	"market-persistence/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// a db agnostic way for batcher to control transaction boundary
type Sink interface {
	InitTx(context.Context) (Tx, error)
	// can add more methods if required
}

type PostgresSink struct {
	pool *pgxpool.Pool
}

func NewPostgresSink() Sink {
	return &PostgresSink{
		pool: db.Pool,
	}
}

func (pg *PostgresSink) InitTx(ctx context.Context) (Tx, error) {
	txn, err := pg.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return nil, err
	}

	return &PostgresTx{
		tx: txn,
	}, nil
}
