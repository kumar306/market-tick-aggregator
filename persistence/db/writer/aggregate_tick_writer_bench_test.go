package writer

import (
	"context"
	"market-persistence/batcher/util"
	"market-persistence/db/model"
	"testing"
	"time"
)

// benchTx is a minimal Tx stub for benchmarking.
// it doesn't accumulate call history across calls, so it doesn't
// contaminate the allocation measurement with its own bookkeeping overhead.
type benchTx struct{}

func (benchTx) Exec(_ context.Context, _ string, _ ...any) (int64, error) { return 1, nil }

func (benchTx) CopyFrom(_ context.Context, _ string, _ []string, rowSrc util.CopyRowSource) (int64, error) {
	count := 0
	for rowSrc.Next() {
		if _, err := rowSrc.Values(); err != nil {
			return 0, err
		}
		count++
	}
	return int64(count), nil
}

func (benchTx) Commit(context.Context) error   { return nil }
func (benchTx) Rollback(context.Context) error { return nil }

func benchAggregatedTicks(n int) []*model.AggregatedTick {
	rows := make([]*model.AggregatedTick, n)
	now := time.Now()
	for i := range rows {
		rows[i] = &model.AggregatedTick{
			Exchange:  "binance",
			Symbol:    "BTCUSDT",
			WindowId:  "5s",
			StartTsMs: now.UnixMilli(),
			EndTsMs:   now.UnixMilli() + 5000,
			StartTs:   now,
			EndTs:     now.Add(5 * time.Second),
			Open:      65000, Close: 65010, High: 65050, Low: 64950,
			VWAP: 65005, RollingVWAP: 65003, TWAP: 65002, Microprice: 65004,
			Volume: 12.5, RollingVolume: 100.2, VolumeAcceleration: 0.4,
			Volatility: 0.02, Atr: 15.3, Ema: 65001, Sma: 65000,
			LogReturn: 0.0001, SimpleReturn: 0.0001,
		}
	}
	return rows
}

// bench test that measures the persistence hot path: the
// staging-table DDL/insert Exec calls and iterating the COPY row source for
// a batch. Uses a mock tx to isolate the writer's own allocation behavior from network/DB cost.
// random batch size
func BenchmarkFlushAggregateTicks(b *testing.B) {
	initTestMetrics()
	tx := benchTx{}
	rows := benchAggregatedTicks(500)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := FlushAggregateTicks(context.Background(), tx, rows); err != nil {
			b.Fatalf("FlushAggregateTicks failed: %v", err)
		}
	}
}
