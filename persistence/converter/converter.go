package converter

import (
	"market-persistence/db/model"
	"market-persistence/proto/generated"
	"shared/logger"
	"time"

	"google.golang.org/protobuf/proto"
)

// pipeline owns a converter to convert the proto message into rows to insert into db

type Converter[U any] interface {
	Convert([]byte) (U, error)
}

type TickConverter struct{}
type BookConverter struct{}

func NewTickConverter() Converter[*model.AggregatedTick] {
	return &TickConverter{}
}

func NewBookConverter() Converter[*model.OrderbookFlush] {
	return &BookConverter{}
}

func (t *TickConverter) Convert(input []byte) (*model.AggregatedTick, error) {
	output := &generated.AggregatedTick{}
	// unmarshal and convert

	if err := proto.Unmarshal(input, output); err != nil {
		logger.Log.Error("Error in unmarshalling bytestream to proto", "error", err)
		return nil, err
	}

	if output.PriceMetrics == nil {
		output.PriceMetrics = &generated.PriceMetrics{}
	}

	if output.PriceMetrics.Ohlc == nil {
		output.PriceMetrics.Ohlc = &generated.OHLC{}
	}

	if output.VolumeMetrics == nil {
		output.VolumeMetrics = &generated.VolumeMetrics{}
	}

	if output.VolatilityMetrics == nil {
		output.VolatilityMetrics = &generated.VolatilityMetrics{}
	}

	if output.TrendMetrics == nil {
		output.TrendMetrics = &generated.TrendMetrics{}
	}

	return &model.AggregatedTick{
		Exchange:           output.Exchange,
		Symbol:             output.Symbol,
		WindowId:           output.WindowId,
		StartTsMs:          output.StartTsMs,
		EndTsMs:            output.EndTsMs,
		StartTs:            time.UnixMilli(output.StartTsMs),
		EndTs:              time.UnixMilli(output.EndTsMs),
		Open:               output.PriceMetrics.Ohlc.Open,
		Close:              output.PriceMetrics.Ohlc.Close,
		Low:                output.PriceMetrics.Ohlc.Low,
		High:               output.PriceMetrics.Ohlc.High,
		VWAP:               output.PriceMetrics.Vwap,
		RollingVWAP:        output.PriceMetrics.RollingVwap,
		TWAP:               output.PriceMetrics.Twap,
		Microprice:         output.PriceMetrics.Microprice,
		Volume:             output.VolumeMetrics.Volume,
		RollingVolume:      output.VolumeMetrics.RollingVolume,
		VolumeAcceleration: output.VolumeMetrics.VolumeAcceleration,
		Volatility:         output.VolatilityMetrics.Volatility,
		Atr:                output.VolatilityMetrics.Atr,
		Ema:                output.TrendMetrics.Ema,
		Sma:                output.TrendMetrics.Sma,
		LogReturn:          output.TrendMetrics.LogReturn,
		SimpleReturn:       output.TrendMetrics.SimpleReturn,
	}, nil

}

func (b *BookConverter) Convert(input []byte) (*model.OrderbookFlush, error) {
	// unmarshal and convert
	output := &generated.OrderbookFlush{}

	if err := proto.Unmarshal(input, output); err != nil {
		logger.Log.Error("Error in unmarshalling orderbook snapshot bytestream to proto", "error", err)
		return nil, err
	}

	var levelRows []*model.OrderbookFlushLevelRow = make([]*model.OrderbookFlushLevelRow, 0)
	for idx, bid := range output.Bids {
		levelRows = append(levelRows, &model.OrderbookFlushLevelRow{
			LevelIndex: idx,
			Side:       "B",
			Price:      bid.Price,
			Volume:     bid.Volume,
		})
	}

	for idx, ask := range output.Asks {
		levelRows = append(levelRows, &model.OrderbookFlushLevelRow{
			LevelIndex: idx,
			Side:       "A",
			Price:      ask.Price,
			Volume:     ask.Volume,
		})
	}

	if output.BestBid == nil {
		output.BestBid = &generated.OrderbookFlush_BookLevel{}
	}

	if output.BestAsk == nil {
		output.BestAsk = &generated.OrderbookFlush_BookLevel{}
	}

	book := &model.OrderbookFlush{
		FlushRow: &model.OrderbookFlushRow{
			Exchange:        output.Exchange,
			Symbol:          output.Symbol,
			EventTimeMillis: output.EventTimeMillis,
			EventTime:       time.UnixMilli(output.EventTimeMillis),
			BestBidPrice:    output.BestBid.Price,
			BestBidVolume:   output.BestBid.Volume,
			BestAskPrice:    output.BestAsk.Price,
			BestAskVolume:   output.BestAsk.Volume,
			Spread:          output.Spread,
		},
		LevelRows: levelRows,
	}

	return book, nil
}
