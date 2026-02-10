package writer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"market-persistence/db/model"
	"shared/metrics"
)

var initMetricsOnce sync.Once

func initTestMetrics() {
	initMetricsOnce.Do(func() {
		metrics.InitPersistenceMetrics()
	})
}

type execCall struct {
	sql  string
	args []any
}

// will use a mock tx wrapper to count number of calls with sql, arg to track table insert made to tx exec
// can use it to throw error at specific exec call in the case of real processing
type mockTx struct {
	execCalls     []execCall
	execErrAtCall int
	execErr       error
}

func (m *mockTx) Exec(_ context.Context, sql string, args ...any) (int64, error) {
	m.execCalls = append(m.execCalls, execCall{sql: sql, args: args})
	if m.execErrAtCall > 0 && len(m.execCalls) == m.execErrAtCall {
		return 0, m.execErr
	}
	return 1, nil
}

func (m *mockTx) Commit(context.Context) error   { return nil }
func (m *mockTx) Rollback(context.Context) error { return nil }

func TestFlushAggregateTicksSuccess(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{}
	row := &model.AggregatedTick{
		Exchange:           "binance",
		Symbol:             "BTCUSDT",
		WindowId:           "1m",
		StartTsMs:          1000,
		EndTsMs:            2000,
		StartTs:            time.UnixMilli(1000),
		EndTs:              time.UnixMilli(2000),
		Open:               10,
		Close:              11,
		Low:                9,
		High:               12,
		VWAP:               10.5,
		RollingVWAP:        10.4,
		TWAP:               10.3,
		Microprice:         10.2,
		Volume:             100,
		RollingVolume:      400,
		VolumeAcceleration: 1.2,
		Volatility:         0.8,
		Atr:                0.7,
		Ema:                10.1,
		Sma:                10.0,
		LogReturn:          0.01,
		SimpleReturn:       0.02,
	}

	if err := FlushAggregateTicks(context.Background(), tx, []*model.AggregatedTick{row}); err != nil {
		t.Fatalf("FlushAggregateTicks() error = %v, want nil", err)
	}

	if len(tx.execCalls) != 1 {
		t.Fatalf("exec call count = %d, want 1", len(tx.execCalls))
	}
	if !strings.Contains(tx.execCalls[0].sql, "INSERT INTO aggregated_ticks") {
		t.Fatalf("unexpected sql: %s", tx.execCalls[0].sql)
	}
	if len(tx.execCalls[0].args) != 24 {
		t.Fatalf("arg count = %d, want 24", len(tx.execCalls[0].args))
	}
}

func TestFlushAggregateTicksExecError(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{
		execErrAtCall: 1,
		execErr:       errors.New("db exec failed"),
	}
	err := FlushAggregateTicks(context.Background(), tx, []*model.AggregatedTick{{}})
	if err == nil {
		t.Fatalf("FlushAggregateTicks() error = nil, want non-nil")
	}
}

func TestFlushOrderbookSuccess(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{}
	row := &model.OrderbookFlush{
		FlushRow: &model.OrderbookFlushRow{
			Exchange:        "okx",
			Symbol:          "ETHUSDT",
			EventTimeMillis: 12345,
			EventTime:       time.UnixMilli(12345),
			BestBidPrice:    100,
			BestBidVolume:   2,
			BestAskPrice:    101,
			BestAskVolume:   3,
			Spread:          1,
		},
		LevelRows: []*model.OrderbookFlushLevelRow{
			{LevelIndex: 0, Side: "B", Price: 100, Volume: 2},
			{LevelIndex: 0, Side: "A", Price: 101, Volume: 3},
		},
	}

	if err := FlushOrderbook(context.Background(), tx, []*model.OrderbookFlush{row}); err != nil {
		t.Fatalf("FlushOrderbook() error = %v, want nil", err)
	}

	if len(tx.execCalls) != 3 {
		t.Fatalf("exec call count = %d, want 3 (1 parent + 2 level)", len(tx.execCalls))
	}
	if !strings.Contains(tx.execCalls[0].sql, "INSERT INTO orderbook_flushes") {
		t.Fatalf("unexpected parent insert sql")
	}
	if !strings.Contains(tx.execCalls[1].sql, "INSERT INTO orderbook_flush_levels") {
		t.Fatalf("unexpected level insert sql")
	}
	if !strings.Contains(tx.execCalls[2].sql, "INSERT INTO orderbook_flush_levels") {
		t.Fatalf("unexpected level insert sql")
	}
}

func TestFlushOrderbookParentInsertError(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{
		execErrAtCall: 1,
		execErr:       errors.New("parent insert failed"),
	}
	err := FlushOrderbook(context.Background(), tx, []*model.OrderbookFlush{{
		FlushRow: &model.OrderbookFlushRow{},
	}})
	if err == nil {
		t.Fatalf("FlushOrderbook() error = nil, want non-nil")
	}
}

func TestFlushOrderbookLevelInsertError(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{
		execErrAtCall: 2,
		execErr:       errors.New("level insert failed"),
	}
	err := FlushOrderbook(context.Background(), tx, []*model.OrderbookFlush{{
		FlushRow: &model.OrderbookFlushRow{},
		LevelRows: []*model.OrderbookFlushLevelRow{
			{LevelIndex: 0, Side: "B", Price: 1, Volume: 1},
		},
	}})
	if err == nil {
		t.Fatalf("FlushOrderbook() error = nil, want non-nil")
	}
}
