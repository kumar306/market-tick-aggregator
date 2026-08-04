package kafka_test

import (
	"bytes"
	"encoding/gob"
	"market-aggregator/constants"
	"market-aggregator/internal/aggmetrics"
	"market-aggregator/kafka"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWindowCheckpointGobRoundTrip(t *testing.T) {
	original := map[constants.MetricName]constants.Metric{
		"ohlc": &aggmetrics.OHLC{Open: 100, High: 110, Low: 95, Close: 105, OpenSet: true},
		"atr":  &aggmetrics.ATR{Value: 4.2, Alpha: 1.0 / 14, PrevClose: 104, Init: true},
		"rolling_vwap": &aggmetrics.RollingVWAP{
			Buckets:        []aggmetrics.VWAPBucket{{SumPV: 10, SumV: 2}, {SumPV: 20, SumV: 4}},
			Idx:            1,
			BucketSizeMs:   1000,
			LastBucketTsMs: 5000,
			TotalSumPV:     30,
			TotalSumV:      6,
		},
	}

	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(kafka.WindowCheckpoint{Metrics: original}))

	var decoded kafka.WindowCheckpoint
	require.NoError(t, gob.NewDecoder(&buf).Decode(&decoded))

	require.Equal(t, original["ohlc"], decoded.Metrics["ohlc"])
	require.Equal(t, original["atr"], decoded.Metrics["atr"])
	require.Equal(t, original["rolling_vwap"], decoded.Metrics["rolling_vwap"])
}
