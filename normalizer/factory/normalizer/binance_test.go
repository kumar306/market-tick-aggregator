package normalizer

import (
	"market-normalizer/constants"
	"market-normalizer/proto/generated"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/require"
)

func TestBinanceAggTradeNormalizerSetsTradePriceAsOHLC(t *testing.T) {
	n := &BinanceAggTradeNormalizer{}

	msg := &constants.PipelineMessage{
		Exchange: constants.Binance,
		Channel:  constants.AggTrade,
		Symbol:   "BTCUSDT",
		SeqId:    12345,
		RawMessage: &constants.BinanceAggTradeMsg{
			EventTime:  1711734000123,
			Symbol:     "BTCUSDT",
			AggTradeID: 12345,
			Price:      "68462.30",
			Quantity:   "0.125",
		},
	}

	raw, err := n.Normalize(msg)
	require.NoError(t, err)

	out := &generated.NormalizedTicker{}
	err = proto.Unmarshal(raw, out)
	require.NoError(t, err)

	require.Equal(t, 68462.30, out.Price)
	require.Equal(t, 0.125, out.Volume)
	require.Equal(t, out.Price, out.Open)
	require.Equal(t, out.Price, out.Close)
	require.Equal(t, out.Price, out.Low)
	require.Equal(t, out.Price, out.High)
	require.Equal(t, int64(1711734000123), out.EventTsMillis)
	require.Equal(t, int64(12345), out.SeqId)
}
