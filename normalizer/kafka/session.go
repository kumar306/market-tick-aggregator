package kafka

import (
	"context"
	"fmt"
	"market-normalizer/constants"
	"os"
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

	session, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.RequireStableFetchOffsets(),
		kgo.TransactionalID(transactionalID),
		kgo.TransactionTimeout(10*time.Second),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.MaxBufferedRecords(cfg.MaxBufferRecords),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, nil)),
	)
	if err != nil {
		return nil, err
	}

	if err := session.Client().Ping(ctx); err != nil {
		session.Close()
		return nil, err
	}

	return session, nil
}
