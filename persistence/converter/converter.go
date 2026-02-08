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

	generated := &generated.AggregatedTick{}
	// unmarshal and convert

	if err := proto.Unmarshal(input, generated); err != nil {
		logger.Log.Error("Error in unmarshalling bytestream to proto", "error", err)
		return nil, err
	}

	return &model.AggregatedTick{
		Exchange:           generated.Exchange,
		Symbol:             generated.Symbol,
		WindowId:           generated.WindowId,
		StartTsMs:          generated.StartTsMs,
		EndTsMs:            generated.EndTsMs,
		StartTs:            time.UnixMilli(generated.StartTsMs),
		EndTs:              time.UnixMilli(generated.EndTsMs),
		Open:               generated.PriceMetrics.Ohlc.Open,
		Close:              generated.PriceMetrics.Ohlc.Close,
		Low:                generated.PriceMetrics.Ohlc.Low,
		High:               generated.PriceMetrics.Ohlc.High,
		VWAP:               generated.PriceMetrics.Vwap,
		RollingVWAP:        generated.PriceMetrics.RollingVwap,
		TWAP:               generated.PriceMetrics.Twap,
		Microprice:         generated.PriceMetrics.Microprice,
		Volume:             generated.VolumeMetrics.Volume,
		RollingVolume:      generated.VolumeMetrics.RollingVolume,
		VolumeAcceleration: generated.VolumeMetrics.VolumeAcceleration,
		Volatility:         generated.VolatilityMetrics.Volatility,
		Atr:                generated.VolatilityMetrics.Atr,
		Ema:                generated.TrendMetrics.Ema,
		Sma:                generated.TrendMetrics.Sma,
		LogReturn:          generated.TrendMetrics.LogReturn,
		SimpleReturn:       generated.TrendMetrics.SimpleReturn,
	}, nil

}

func (b *BookConverter) Convert(input []byte) (*model.OrderbookFlush, error) {

	// unmarshal and convert
	generated := &generated.OrderbookFlush{}

	if err := proto.Unmarshal(input, generated); err != nil {
		logger.Log.Error("Error in unmarshalling orderbook snapshot bytestream to proto", "error", err)
		return nil, err
	}

	var levelRows []*model.OrderbookFlushLevelRow = make([]*model.OrderbookFlushLevelRow, 0)
	for idx, bid := range generated.Bids {
		levelRows = append(levelRows, &model.OrderbookFlushLevelRow{
			LevelIndex: idx,
			Side:       "B",
			Price:      bid.Price,
			Volume:     bid.Volume,
		})
	}

	for idx, ask := range generated.Asks {
		levelRows = append(levelRows, &model.OrderbookFlushLevelRow{
			LevelIndex: idx,
			Side:       "A",
			Price:      ask.Price,
			Volume:     ask.Volume,
		})
	}

	book := &model.OrderbookFlush{
		FlushRow: &model.OrderbookFlushRow{
			Exchange:        generated.Exchange,
			Symbol:          generated.Symbol,
			EventTimeMillis: generated.EventTimeMillis,
			EventTime:       time.UnixMilli(generated.EventTimeMillis),
			BestBidPrice:    generated.BestBid.Price,
			BestBidVolume:   generated.BestBid.Volume,
			BestAskPrice:    generated.BestAsk.Price,
			BestAskVolume:   generated.BestAsk.Volume,
			Spread:          generated.Spread,
		},
		LevelRows: levelRows,
	}

	return book, nil
}
