package internal_test

import (
	"context"
	"market-adapter/constants"
	"market-adapter/internal"
	"math"
	"shared/metrics"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// i'll simulate a failed connection by passing in random invalid url
// supervisor to be created and start its child loop on its channel
// send StatusNew event to the channel. it tries to connect
// connect fails as websocket closes connection instantly.
// it should queue retry
// verify:
// i. retry happens maxRetry number of times
// ii. store timestamps in a slice and calculate the time difference b/w the timestamps and ensure the intervals are exponentially increasing within jitter

func Test_RetryConnection(t *testing.T) {
	metrics.InitAdapterMetrics()
	feed := &constants.Feed{
		Name: "binance",
		Url:  "http://localhost:5999999/ws",
		Streams: []*constants.Stream{
			{
				Name:            "binance",
				Channel:         "aggTrade",
				ProductIds:      []string{"btcusdt"},
				KafkaTopic:      "binance_raw_aggtrade",
				MaxRetries:      4,
				BaseDelay:       1,
				MaxJitterMillis: 500,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	supervisor := &constants.Supervisor{
		Wg:           &sync.WaitGroup{},
		Ctx:          ctx,
		Cancel:       cancel,
		StatusChan:   make(chan constants.Status, 5),
		LastPongTime: time.Now(),
	}

	var retryTimes []time.Time
	var mu sync.Mutex

	// log retry timestamps
	internal.RetryHook = func() {
		mu.Lock()
		retryTimes = append(retryTimes, time.Now())
		mu.Unlock()
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go internal.SupervisorLoop(feed, feed.Streams[0], supervisor, &wg)

	// try to connect and it will fail and queue retry maxRetry times
	supervisor.StatusChan <- constants.StatusNew

	wg.Wait()

	require.Equal(t, feed.Streams[0].MaxRetries, len(retryTimes), "Number of retries should be equal to max retries")

	for i := range retryTimes {
		if i > 0 {
			interval := retryTimes[i].Sub(retryTimes[i-1])
			t.Logf("Got the interval %v: %v", i, interval)
			baseDelay := time.Duration(feed.Streams[0].BaseDelay) * time.Second
			expectedMin := baseDelay * time.Duration(math.Pow(2, float64(i)))
			expectedMax := expectedMin + time.Duration(500)*time.Millisecond // jitter
			t.Logf("Expected min for interval %v: %v, Expected max: %v", i, expectedMin, expectedMax)
			require.GreaterOrEqual(t, interval, expectedMin, "Expected interval to be more than expected min")
			require.LessOrEqual(t, interval, expectedMax, "Expected interval to be less than the expected max")
		}
	}
}
