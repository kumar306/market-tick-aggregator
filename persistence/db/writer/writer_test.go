package writer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"market-persistence/batcher/util"
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

type copyCall struct {
	table    string
	columns  []string
	rowCount int
}

// will use a mock tx wrapper to count number of calls with sql, arg to track table insert made to tx exec, copy from
// can use it to throw error at specific exec call in the case of real processing
type mockTx struct {
	execCalls     []execCall
	execErrAtCall int
	execErr       error
	copyCalls     []copyCall
	copyErr       error
}

func (m *mockTx) Exec(_ context.Context, sql string, args ...any) (int64, error) {
	m.execCalls = append(m.execCalls, execCall{sql: sql, args: args})
	if m.execErrAtCall > 0 && len(m.execCalls) == m.execErrAtCall {
		return 0, m.execErr
	}
	return 1, nil
}

func (m *mockTx) CopyFrom(_ context.Context, tableName string, columnNames []string, rowSrc util.CopyRowSource) (int64, error) {
	if m.copyErr != nil {
		return 0, m.copyErr
	}
	count := 0
	for rowSrc.Next() {
		if _, err := rowSrc.Values(); err != nil {
			return 0, err
		}
		count++
	}
	m.copyCalls = append(m.copyCalls, copyCall{table: tableName, columns: columnNames, rowCount: count})
	return int64(count), nil
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

	// exec 1: create staging table, exec 2: insert-from-staging
	if len(tx.execCalls) != 2 {
		t.Fatalf("exec call count = %d, want 2 (staging DDL + insert-from-staging)", len(tx.execCalls))
	}
	if !strings.Contains(tx.execCalls[0].sql, "CREATE TEMP TABLE") {
		t.Fatalf("expected first exec to create the staging table, got: %s", tx.execCalls[0].sql)
	}
	if !strings.Contains(tx.execCalls[1].sql, "INSERT INTO aggregated_ticks") {
		t.Fatalf("expected second exec to insert from staging, got: %s", tx.execCalls[1].sql)
	}

	if len(tx.copyCalls) != 1 {
		t.Fatalf("copy call count = %d, want 1", len(tx.copyCalls))
	}
	if tx.copyCalls[0].rowCount != 1 {
		t.Fatalf("copied row count = %d, want 1", tx.copyCalls[0].rowCount)
	}
	if len(tx.copyCalls[0].columns) != 24 {
		t.Fatalf("copy column count = %d, want 24", len(tx.copyCalls[0].columns))
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

func TestFlushAggregateTicksCopyError(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{
		copyErr: errors.New("copy failed"),
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

	// exec 1: create staging_orderbook_flushes, exec 2: create staging_orderbook_flush_levels,
	// exec 3: insert-from-staging orderbook_flushes, exec 4: insert-from-staging orderbook_flush_levels
	if len(tx.execCalls) != 4 {
		t.Fatalf("exec call count = %d, want 4 (2 staging DDLs + 2 insert-from-staging)", len(tx.execCalls))
	}
	if !strings.Contains(tx.execCalls[0].sql, "CREATE TEMP TABLE") || !strings.Contains(tx.execCalls[0].sql, "staging_orderbook_flushes") {
		t.Fatalf("expected first exec to create the flushes staging table, got: %s", tx.execCalls[0].sql)
	}
	if !strings.Contains(tx.execCalls[1].sql, "CREATE TEMP TABLE") || !strings.Contains(tx.execCalls[1].sql, "staging_orderbook_flush_levels") {
		t.Fatalf("expected second exec to create the levels staging table, got: %s", tx.execCalls[1].sql)
	}
	if !strings.Contains(tx.execCalls[2].sql, "INSERT INTO orderbook_flushes") {
		t.Fatalf("expected third exec to insert flushes from staging, got: %s", tx.execCalls[2].sql)
	}
	if !strings.Contains(tx.execCalls[3].sql, "INSERT INTO orderbook_flush_levels") {
		t.Fatalf("expected fourth exec to insert levels from staging, got: %s", tx.execCalls[3].sql)
	}

	if len(tx.copyCalls) != 2 {
		t.Fatalf("copy call count = %d, want 2", len(tx.copyCalls))
	}
	if tx.copyCalls[0].table != "staging_orderbook_flushes" || tx.copyCalls[0].rowCount != 1 {
		t.Fatalf("unexpected flushes copy call: %+v", tx.copyCalls[0])
	}
	if tx.copyCalls[1].table != "staging_orderbook_flush_levels" || tx.copyCalls[1].rowCount != 2 {
		t.Fatalf("unexpected levels copy call: %+v", tx.copyCalls[1])
	}
}

func TestFlushOrderbookExecError(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{
		execErrAtCall: 1,
		execErr:       errors.New("staging ddl failed"),
	}
	err := FlushOrderbook(context.Background(), tx, []*model.OrderbookFlush{{
		FlushRow: &model.OrderbookFlushRow{},
	}})
	if err == nil {
		t.Fatalf("FlushOrderbook() error = nil, want non-nil")
	}
}

func TestFlushOrderbookCopyError(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{
		copyErr: errors.New("copy failed"),
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
