package integration_test

import (
	"context"
	"market-orderbook/constants"
	"market-orderbook/internal/testcontainers"
	"market-orderbook/kafka"
	"market-orderbook/proto/generated"
	"market-orderbook/redis"
	"market-orderbook/worker"
	"os"
	"shared/metrics"
	"sync"
	"testing"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

var metricsOnce sync.Once

func initMetrics() {
	metricsOnce.Do(func() {
		metrics.InitOrderbookMetrics()
	})
}

func TestOrderbookIntegration_EndToEndSnapshotAndFlush(t *testing.T) {
	initMetrics()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	upstreamTopic := "orderbook.upstream.int"
	downstreamTopic := "orderbook.downstream.int"

	client, kafkaContainer := testcontainers.StartKafka(ctx, t, []string{upstreamTopic, downstreamTopic})
	defer client.Close()
	defer func() {
		if err := tc.TerminateContainer(kafkaContainer); err != nil {
			t.Fatalf("Error terminating kafka container: %v", err)
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil || len(brokers) == 0 {
		t.Fatalf("Error getting kafka brokers: %v", err)
	}

	downstreamClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("orderbook-downstream-group"),
		kgo.ConsumeTopics(downstreamTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("Error creating downstream client: %v", err)
	}
	defer downstreamClient.Close()

	kafka.Client = client
	kafka.UpstreamTopic = upstreamTopic
	kafka.DownstreamTopic = downstreamTopic

	redisContainer, addr := testcontainers.StartRedis(ctx, t)
	defer func() {
		if err := tc.TerminateContainer(redisContainer); err != nil {
			t.Fatalf("Error terminating redis container: %v", err)
		}
	}()

	if err := os.Setenv(redis.REDIS_ADDR, addr); err != nil {
		t.Fatalf("Error setting redis addr env: %v", err)
	}
	if err := os.Setenv(redis.REDIS_PASSWORD, ""); err != nil {
		t.Fatalf("Error setting redis password env: %v", err)
	}

	redis.InitRedis(&constants.RedisConfig{
		TtlMinutes:   1,
		PoolSize:     2,
		MinIdleConns: 1,
	})

	updateCh := make(chan *constants.DispatchRecord, 20)
	updateAckCh := make(chan *constants.Ack, 20)

	coord := kafka.NewCoordinator(1, []chan *constants.Ack{updateAckCh})
	w := worker.NewWorker(0, ctx, 10, 15, updateCh, coord.FlushAckChannel, updateAckCh)
	w.FlushDepth = 5
	w.SnapshotPrepareIntervalSeconds = 1

	go w.Run()
	go coord.Run(ctx, client)

	update := &generated.NormalizedBook{
		Exchange:        "coinbase",
		Symbol:          "ETH-USD",
		EventTimeMillis: 1234,
		Bids: []*generated.NormalizedBook_BookLevel{
			{Price: 100, Volume: 1},
			{Price: 99, Volume: 2},
		},
		Asks: []*generated.NormalizedBook_BookLevel{
			{Price: 101, Volume: 1},
			{Price: 102, Volume: 3},
		},
	}

	updateCh <- &constants.DispatchRecord{
		Event:     constants.ProcessEvent,
		Partition: 0,
		Offset:    5,
		Update:    update,
		Exchange:  "coinbase",
		Symbol:    "ETH-USD",
		TsMs:      1234,
	}

	updateCh <- &constants.DispatchRecord{
		Event: constants.SnapshotRequestEvent,
	}

	coord.StartEpoch(1, map[int]struct{}{0: {}})
	updateCh <- &constants.DispatchRecord{
		Event:      constants.FlushEvent,
		FlushEpoch: 1,
	}

	key := "coinbase:ETH-USD"

	var snapshotBytes []byte
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		t.Logf("Trying to get snapshot from redis")
		data, _ := redis.GetSnapshot(ctx, key)
		if len(data) > 0 {
			snapshotBytes = data
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if len(snapshotBytes) == 0 {
		t.Fatalf("timed out waiting for snapshot in redis")
	}

	snapshot := &generated.OrderBookSnapshot{}
	if err := proto.Unmarshal(snapshotBytes, snapshot); err != nil {
		t.Fatalf("error unmarshalling snapshot: %v", err)
	}

	if snapshot.Exchange != "coinbase" || snapshot.Symbol != "ETH-USD" {
		t.Fatalf("unexpected snapshot identity: %s:%s", snapshot.Exchange, snapshot.Symbol)
	}
	if len(snapshot.Bids) == 0 || len(snapshot.Asks) == 0 {
		t.Fatalf("expected snapshot bids/asks to be populated")
	}

	gotDownstream := false
	downDeadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(downDeadline) && !gotDownstream {
		fetches := downstreamClient.PollFetches(ctx)
		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			if string(rec.Key) != key {
				continue
			}
			flush := &generated.OrderbookFlush{}
			if err := proto.Unmarshal(rec.Value, flush); err != nil {
				t.Fatalf("error unmarshalling downstream flush: %v", err)
			}
			if flush.Exchange != "coinbase" || flush.Symbol != "ETH-USD" {
				t.Fatalf("unexpected downstream flush identity: %s:%s", flush.Exchange, flush.Symbol)
			}
			gotDownstream = true
			break
		}
	}

	if !gotDownstream {
		t.Fatalf("timed out waiting for downstream flush record")
	}
}
