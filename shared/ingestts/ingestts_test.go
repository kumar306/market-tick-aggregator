package ingestts

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFind_RoundTrip(t *testing.T) {
	headers := []kgo.RecordHeader{{Key: "src", Value: []byte{1}}, Header(1_700_000_123_456)}
	ms, ok := Find(headers)
	require.True(t, ok)
	require.Equal(t, int64(1_700_000_123_456), ms)
}

func TestFind_MissingOrMalformedIsNotAMeasurement(t *testing.T) {
	_, ok := Find(nil)
	require.False(t, ok)

	_, ok = Find([]kgo.RecordHeader{{Key: HeaderKey, Value: []byte{1, 2, 3}}})
	require.False(t, ok, "a header of the wrong length must not be read as a time")
}
