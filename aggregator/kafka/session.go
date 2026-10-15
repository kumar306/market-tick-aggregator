package kafka

import (
	"context"
	"fmt"
	"market-aggregator/constants"
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
func NewWorkerSession(ctx context.Context, cfg *constants.KafkaConfig, workerIdx int) (*kgo.GroupTransactSession, error) {
	transactionalID := fmt.Sprintf("%s-worker-%d", cfg.ConsumerGroup, workerIdx)

	authOpts, _ := kafkaauth.IAMOpts(ctx)
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapServers...),
		kgo.ConsumeTopics(cfg.TopicConfig.Upstream),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.TransactionalID(transactionalID),
		// keep under the group session timeout so a transaction never outlives the rebalance window as per franz-go docs
		kgo.TransactionTimeout(10 * time.Second),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.MaxBufferedRecords(cfg.MaxBufferRecords),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, nil)),
	}, authOpts...)

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

// NewWorkerClient is the at-least-once counterpart of NewWorkerSession: a plain group consumer plus an idempotent
// producer, no transactions. Offsets are never auto-committed; the worker commits them itself, only after the
// checkpoint that covers them has been acked.
func NewWorkerClient(ctx context.Context, cfg *constants.KafkaConfig, workerIdx int, extra ...kgo.Opt) (*kgo.Client, error) {
	authOpts, _ := kafkaauth.IAMOpts(ctx)
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapServers...),
		kgo.ConsumeTopics(cfg.TopicConfig.Upstream),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.DisableAutoCommit(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.MaxBufferedRecords(cfg.MaxBufferRecords),
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
