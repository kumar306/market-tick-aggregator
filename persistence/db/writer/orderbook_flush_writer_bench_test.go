package writer

import (
	"context"
	"market-persistence/db/model"
	"testing"
	"time"
)

// builds n symbols' worth of flush rows, each with a
// top 15 levels per side depth
func benchOrderbookFlushes(n, depthPerSide int) []*model.OrderbookFlush {
	rows := make([]*model.OrderbookFlush, n)
	now := time.Now()

	for i := range rows {
		levels := make([]*model.OrderbookFlushLevelRow, 0, depthPerSide*2)
		for l := 0; l < depthPerSide; l++ {
			levels = append(levels,
				&model.OrderbookFlushLevelRow{LevelIndex: l, Side: "bid", Price: 65000 - float64(l), Volume: 1.5},
				&model.OrderbookFlushLevelRow{LevelIndex: l, Side: "ask", Price: 65001 + float64(l), Volume: 1.2},
			)
		}

		rows[i] = &model.OrderbookFlush{
			FlushRow: &model.OrderbookFlushRow{
				Exchange:        "binance",
				Symbol:          "BTCUSDT",
				EventTimeMillis: now.UnixMilli(),
				EventTime:       now,
				BestBidPrice:    65000,
				BestBidVolume:   1.5,
				BestAskPrice:    65001,
				BestAskVolume:   1.2,
				Spread:          1,
			},
			LevelRows: levels,
		}
	}
	return rows
}

// measures the orderbook-flush persistence path for
// a representative single-symbol flush (1 parent row + 30 level rows, top-15
// per side)
// a fixed 6 round trips per batch (2 staging DDLs, 2 COPYs, 2 insert from staging execs)
func BenchmarkFlushOrderbook(b *testing.B) {
	initTestMetrics()
	tx := benchTx{}
	rows := benchOrderbookFlushes(1, 15)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := FlushOrderbook(context.Background(), tx, rows); err != nil {
			b.Fatalf("FlushOrderbook failed: %v", err)
		}
	}
}
