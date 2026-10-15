package kafka

import (
	"context"
	"fmt"
	"market-normalizer/constants"
	"os"
	"shared/kafkaauth"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// one GroupTransactSession per worker. Each session is an independent member of the same consumer group.
// group coordinator hands each member a disjoint set of partitions - per worker txn is safe as they process disjoint parts.
// abort txn on rebalance and downstream should have read_committed level.
// transactionalID must be stable across restarts of the same worker slot
// to fence out zombie instances of the same slot.
//
// extra opts are appended last; tests use them to inject a dialer that simulates network latency.
func NewWorkerSession(ctx context.Context, cfg *constants.KafkaConfig, workerIdx int, extra ...kgo.Opt) (*kgo.GroupTransactSession, error) {
	transactionalID := fmt.Sprintf("%s-worker-%d", cfg.ConsumerGroup, workerIdx)

	authOpts, _ := kafkaauth.IAMOpts(ctx)
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.TransactionalID(transactionalID),
		kgo.TransactionTimeout(10 * time.Second),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// without this, End() holds a lock for 200ms after every commit to guard a rebalance race.
		// With it, Kafka itself blocks that race (KIP-447), so End() returns right after EndTxn.
		kgo.RequireStableFetchOffsets(),
		kgo.MaxBufferedRecords(cfg.MaxBufferRecords),
		kgo.WithHooks(rpcLatencyHook{}),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, nil)),
	}, authOpts...)
	opts = append(opts, extra...)

	session, err := kgo.NewGroupTransactSession(opts...)
	if err != nil {
		return nil, err
	}

	if err := session.Client().Ping(ctx); err != nil {
		session.Close()
		return nil, err
	}

	return session, nil
}

// NewWorkerClient is the at-least-once counterpart of NewWorkerSession: a plain group consumer plus an
// idempotent producer, with no transactions. Offsets are never auto-committed; the worker commits them
// itself, only up to the point where every output has been acked (see Frontier).
// There is no transactional ID, so a zombie instance is not fenced out. Downstream drops the resulting
// duplicates using the source header.
func NewWorkerClient(ctx context.Context, cfg *constants.KafkaConfig, workerIdx int, extra ...kgo.Opt) (*kgo.Client, error) {
	authOpts, _ := kafkaauth.IAMOpts(ctx)
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.DisableAutoCommit(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.MaxBufferedRecords(cfg.MaxBufferRecords),
		kgo.WithHooks(rpcLatencyHook{}),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, nil)),
	}, authOpts...)
	opts = append(opts, extra...)

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, err
	}
	return cl, nil
}
