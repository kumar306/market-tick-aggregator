package constants

import (
	"market-orderbook/proto/generated"

	"github.com/twmb/franz-go/pkg/kgo"
)

type EventType int

// flush event - sends top N price levels to kafka
// snapshot event - occurs every 1 min, snapshot to redis

const (
	ProcessEvent EventType = iota
	FlushEvent
	SnapshotRequestEvent
)

type RedisConfig struct {
	TtlMinutes   int `yaml:"ttl_minutes"`
	PoolSize     int `yaml:"pool_size"`
	MinIdleConns int `yaml:"min_idle_conns"`
}

type DispatchRecord struct {
	Event     EventType
	Partition int32
	Offset    int64
	Record    *kgo.Record
	Update    *generated.NormalizedBook
	Exchange  string
	Symbol    string
	TsMs      int64
}

type SnapshotRecord struct {
	SnapshotOffset int64
}
