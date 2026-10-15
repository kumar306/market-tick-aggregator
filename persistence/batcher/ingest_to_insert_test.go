package batcher

import (
	"context"
	"errors"
	"shared/ingestts"
	"shared/metrics"
	"testing"
	"time"

	"market-persistence/batcher/util"

	"github.com/twmb/franz-go/pkg/kgo"
)

// samples recorded so far for one pipeline label of persistence_ingest_to_insert_seconds
func ingestToInsertSamples(t *testing.T, pipeline string) (count uint64, sum float64) {
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "persistence_ingest_to_insert_seconds" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "pipeline" && l.GetValue() == pipeline {
					return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
				}
			}
		}
	}
	return 0, 0
}

func flushOne(t *testing.T, pipeline string, tx *mockTx, rec *kgo.Record) {
	b := NewBatcher(
		context.Background(), pipeline, 1, time.Second,
		func(context.Context, util.Tx, []int) error { return nil },
		func(int) error { return nil },
		&mockSink{tx: tx},
		&mockEventProcessor{},
	)
	_ = b.HandleBatchAndFlush(&BatchItem[int]{Item: 1, Record: rec})
}

func TestIngestToInsert_RecordedAfterCommit(t *testing.T) {
	initTestMetrics()

	ingestedAgo := 1500 * time.Millisecond
	rec := &kgo.Record{Headers: []kgo.RecordHeader{ingestts.Header(time.Now().Add(-ingestedAgo).UnixMilli())}}
	flushOne(t, "e2e_ok", &mockTx{}, rec)

	count, sum := ingestToInsertSamples(t, "e2e_ok")
	if count != 1 {
		t.Fatalf("expected 1 sample after a committed flush, got %d", count)
	}
	if sum < 1.5 || sum > 2.0 {
		t.Fatalf("expected about 1.5s (the row is visible a moment after it was ingested 1.5s ago), got %.3fs", sum)
	}
}

// rows that never became visible must not be counted as delivered
func TestIngestToInsert_NotRecordedWhenCommitFails(t *testing.T) {
	initTestMetrics()

	rec := &kgo.Record{Headers: []kgo.RecordHeader{ingestts.Header(time.Now().UnixMilli())}}
	flushOne(t, "e2e_commit_fails", &mockTx{commitErr: errors.New("commit failed")}, rec)

	if count, _ := ingestToInsertSamples(t, "e2e_commit_fails"); count != 0 {
		t.Fatalf("a failed commit must record no sample, got %d", count)
	}
}

func TestIngestToInsert_RecordWithoutHeaderIsSkipped(t *testing.T) {
	initTestMetrics()

	flushOne(t, "e2e_no_header", &mockTx{}, &kgo.Record{})

	if count, _ := ingestToInsertSamples(t, "e2e_no_header"); count != 0 {
		t.Fatalf("a record with no ingest header cannot be measured, got %d samples", count)
	}
}
