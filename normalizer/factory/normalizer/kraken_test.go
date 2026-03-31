package normalizer

import (
	"market-normalizer/constants"
	"market-normalizer/proto/generated"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKrakenTickerNormalizerSetsTradePriceAsOpenCloseAndUsesRecordTimestamp(t *testing.T) {
	n := &KrakenTickerNormalizer{}
	recordTs := time.UnixMilli(1711734000123)

	msg := &constants.PipelineMessage{
		Exchange: constants.Kraken,
		Channel:  constants.Ticker,
		Symbol:   "BTC/USD",
		Record: &kgo.Record{
			Timestamp: recordTs,
		},
		RawMessage: &constants.KrakenTickerMsg{
			Channel: constants.Ticker,
			Type:    constants.Snapshot,
			Data: []constants.KrakenTickerData{
				{
					Symbol: "BTC/USD",
					Last:   67660.8,
					Volume: 3254.35898331,
					Low:    65939,
					High:   68500,
				},
			},
		},
	}

	raw, err := n.Normalize(msg)
	require.NoError(t, err)

	out := &generated.NormalizedTicker{}
	err = proto.Unmarshal(raw, out)
	require.NoError(t, err)

	require.Equal(t, 67660.8, out.Price)
	require.Equal(t, 3254.35898331, out.Volume)
	require.Equal(t, out.Price, out.Open)
	require.Equal(t, out.Price, out.Close)
	require.Equal(t, 65939.0, out.Low)
	require.Equal(t, 68500.0, out.High)
	require.Equal(t, recordTs.UnixMilli(), out.EventTsMillis)
}
