package util

import (
	"context"
	"testing"
)

// just testing whether it calls offset commit
func TestKafkaOffsetCommitterCommitOffsetsDelegatesToKafka(t *testing.T) {
	called := 0
	var gotTopic string
	var gotOffsets map[int32]int64
	commitOffsetsPostWrite = func(_ context.Context, topic string, offsets map[int32]int64) {
		called++
		gotTopic = topic
		gotOffsets = offsets
	}

	committer := NewKafkaOffsetCommitter("aggregated_ticks")
	input := map[int32]int64{0: 10, 1: 20}
	if err := committer.CommitOffsets(context.Background(), input); err != nil {
		t.Fatalf("CommitOffsets() error = %v, want nil", err)
	}

	if called != 1 {
		t.Fatalf("delegate call count = %d, wanted 1", called)
	}
	if gotTopic != "aggregated_ticks" {
		t.Fatalf("topic = %q, wanted aggregated_ticks", gotTopic)
	}
	if gotOffsets[1] != 20 {
		t.Fatalf("offset mismatch")
	}
}
