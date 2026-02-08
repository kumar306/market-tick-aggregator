package util

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// db agnostic tx interface wrapper used by batcher, flush fn
type Tx interface {
	Exec(context.Context, string, ...any) (int64, error)
	Commit(context.Context) error
	Rollback(context.Context) error
}

type PostgresTx struct {
	tx pgx.Tx
}

func (t *PostgresTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	ct, err := t.tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

func (t *PostgresTx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *PostgresTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}
