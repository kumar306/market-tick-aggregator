package integration_test

import (
	"context"
	"market-orderbook/backpressure"
	"market-orderbook/constants"
	"market-orderbook/dispatcher"
	"market-orderbook/flush"
	testcontainers "market-orderbook/internal/testcontainers"
	"market-orderbook/kafka"
	"market-orderbook/proto/generated"
	"market-orderbook/redis"
	"market-orderbook/worker"
	"os"
	"shared/logger"
	"testing"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

func TestMainFlowSmoke(t *testing.T) {
	initMetrics()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	g, ctx := errgroup.WithContext(ctx)

	upstreamTopic := "orderbook.upstream.smoke"
	downstreamTopic := "orderbook.downstream.smoke"

	helperClient, kafkaContainer := testcontainers.StartKafka(ctx, t, []string{upstreamTopic, downstreamTopic})
	defer helperClient.Close()
	defer func() {
		if err := tc.TerminateContainer(kafkaContainer); err != nil {
			t.Fatalf("Error terminating kafka container: %v", err)
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil || len(brokers) == 0 {
		t.Fatalf("Error getting kafka brokers: %v", err)
	}

	kafka.Init(ctx, &constants.KafkaConfig{
		BootstrapServers: brokers,
		TopicConfig: constants.TopicConfig{
			Upstream:   upstreamTopic,
			Downstream: downstreamTopic,
		},
		ConsumerGroup:          "orderbook-smoke-group",
		MaxBufferRecords:       1000,
		CBReqCount:             1,
		CBFailureRatio:         0.8,
		ProduceErrorBufferSize: 10,
		FlushIntervalSeconds:   1,
		BackpressureConfig: constants.BackpressureConfig{
			QueueUsageHighThreshold: 0.9,
			QueueUsageLowThreshold:  0.4,
			ConfirmSeconds:          1,
			PollIntervalMs:          100,
		},
	})
	defer kafka.Close()

	downstreamClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("orderbook-downstream-smoke"),
		kgo.ConsumeTopics(downstreamTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("Error creating downstream client: %v", err)
	}
	defer downstreamClient.Close()

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
	if redis.Rdb != nil {
		defer redis.Rdb.Close()
	}

	workerCount := 2
	workerChannels := dispatcher.CreateWorkerChannels(workerCount, 100)
	workerAckChannels := dispatcher.CreateWorkerAckChannels(workerCount, 100)
	dispatchChannel := make(chan *kgo.Record, 100)

	backpressure.InitBP(&constants.BackpressureConfig{
		QueueUsageHighThreshold: 0.9,
		QueueUsageLowThreshold:  0.4,
		ConfirmSeconds:          1,
		PollIntervalMs:          100,
	}, kafka.Client, upstreamTopic, 100)

	coordinator := kafka.NewCoordinator(workerCount, workerAckChannels)

	for idx, ch := range workerChannels {
		w := worker.NewWorker(idx, ctx, ch, coordinator.FlushAckChannel, workerAckChannels[idx])
		w.FlushDepth = 5
		w.SnapshotPrepareIntervalSeconds = 1
		g.Go(func() error {
			w.Run()
			return nil
		})
	}

	g.Go(func() error {
		dispatcher.RunDispatcher(ctx, dispatchChannel, workerChannels)
		return nil
	})

	g.Go(func() error {
		coordinator.Run(ctx, kafka.Client)
		return nil
	})

	g.Go(func() error {
		flush.RunEpochFlushScheduler(ctx, 1, workerChannels, coordinator)
		return nil
	})

	g.Go(func() error {
		kafka.StartConsumer(ctx, dispatchChannel)
		return nil
	})

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

	val, err := proto.Marshal(update)
	if err != nil {
		t.Fatalf("error marshalling update: %v", err)
	}

	kafka.Client.Produce(ctx, &kgo.Record{
		Topic: upstreamTopic,
		Key:   []byte("coinbase:ETH-USD"),
		Value: val,
	}, nil)
	_ = kafka.Client.Flush(ctx)
	t.Logf("Flushed record to upstream topic which goes to dispatcher")

	key := "coinbase:ETH-USD"
	var snapshotBytes []byte
	snapshotDeadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(snapshotDeadline) {
		logger.Log.Info("Trying to get the snapshot before deadline")
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

	t.Logf("In test - got the snapshot from redis")

	gotDownstream := false
	downDeadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(downDeadline) && !gotDownstream {
		t.Logf("Trying to poll fetches in test")
		fetches := downstreamClient.PollFetches(ctx)
		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			if string(rec.Key) == key {
				gotDownstream = true
				t.Logf("Received the record from poll")
				break
			}
		}
	}

	t.Logf("gotDownstream value: %v", gotDownstream)

	cancel()
	_ = g.Wait()

	if !gotDownstream {
		t.Fatalf("timed out waiting for downstream flush record")
	}
}
