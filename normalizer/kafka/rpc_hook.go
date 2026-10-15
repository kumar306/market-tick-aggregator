package kafka

import (
	"shared/metrics"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// rpcLatencyHook times every broker round trip by request type.
// A commit is several calls in sequence (AddOffsetsToTxn, TxnOffsetCommit, Heartbeat, EndTxn), so the per-call
// time tells us whether a slow commit is the network (every call slow) or the broker (only some calls slow).
type rpcLatencyHook struct{}

func (rpcLatencyHook) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, e kgo.BrokerE2E) {
	metrics.Normalizer_BrokerRPCSeconds.WithLabelValues(kmsg.NameForKey(key)).Observe(e.DurationE2E().Seconds())
}
