package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// segregate some logic of consumers so it can be unit tested
func TestBuildOffsetCommitMap(t *testing.T) {
	got := buildOffsetCommitMap("aggregated_ticks", map[int32]int64{
		0: 10,
		3: 25,
	})

	topicMap, ok := got["aggregated_ticks"]
	if !ok {
		t.Fatalf("topic key missing: %+v", got)
	}
	if topicMap[0] != (kgo.EpochOffset{Offset: 11, Epoch: -1}) {
		t.Fatalf("partition 0 mapping mismatch: %+v", topicMap[0])
	}
	if topicMap[3] != (kgo.EpochOffset{Offset: 26, Epoch: -1}) {
		t.Fatalf("partition 3 mapping mismatch: %+v", topicMap[3])
	}
}

func TestGetCommittedOffset(t *testing.T) {
	req := &kmsg.OffsetCommitRequest{
		Topics: []kmsg.OffsetCommitRequestTopic{
			{
				Partitions: []kmsg.OffsetCommitRequestTopicPartition{
					{Partition: 2, Offset: 99},
				},
			},
		},
	}

	got := getCommittedOffset(req, 2)
	if got != 98 {
		t.Fatalf("committed offset = %d, want 98", got)
	}
}

func TestGetCommittedOffsetMissingPartition(t *testing.T) {
	req := &kmsg.OffsetCommitRequest{
		Topics: []kmsg.OffsetCommitRequestTopic{
			{Partitions: []kmsg.OffsetCommitRequestTopicPartition{}},
		},
	}

	got := getCommittedOffset(req, 10)
	if got != 0 {
		t.Fatalf("committed offset = %d, want 0", got)
	}
}

func TestCalculateLag(t *testing.T) {
	tests := []struct {
		name      string
		latest    int64
		committed int64
		exists    bool
		want      int64
	}{
		{name: "normal case", latest: 100, committed: 90, exists: true, want: 10},
		{name: "committed missing", latest: 100, committed: 0, exists: false, want: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateLag(tt.latest, tt.committed, tt.exists)
			if got != tt.want {
				t.Fatalf("calculateLag() = %d, wanted %d", got, tt.want)
			}
		})
	}
}
