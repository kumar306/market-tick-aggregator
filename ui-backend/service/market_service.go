package service

import (
	"context"
	"errors"
	"market-ui-backend/dto"
	"market-ui-backend/repository"
	"shared/logger"
	"time"
)

type MarketService struct {
	repository *repository.MarketRepository
}

func NewMarketService(repository *repository.MarketRepository) *MarketService {
	return &MarketService{repository: repository}
}

func (s *MarketService) GetCandles(ctx context.Context,
	exchange string,
	symbol string,
	from time.Time,
	to time.Time) ([]*dto.CandleDTO, error) {
	rows, err := s.repository.GetCandles(ctx, exchange, symbol, from, to)
	if err != nil {
		return nil, err
	}

	var result []*dto.CandleDTO
	for _, r := range rows {
		result = append(result, &dto.CandleDTO{
			StartTs: r.StartTs,
			EndTs:   r.EndTs,
			Open:    r.Open,
			Low:     r.Low,
			High:    r.High,
			Close:   r.Close,
			Volume:  r.Volume,
		})
	}

	return result, nil
}

func (s *MarketService) GetMetrics(ctx context.Context,
	exchange string,
	symbol string,
	windows []string,
	metrics []string,
	from, to time.Time) (*dto.MetricResultDTO, error) {
	// response: json of exchange, symbol -> windows : {"5m": [{metric: 'name', value: val, timestamp: ts}] }
	rows, err := s.repository.GetMetrics(ctx, exchange, symbol, windows, metrics, from, to)
	if err != nil {
		return nil, err
	}

	result := &dto.MetricResultDTO{}
	result.Exchange = exchange
	result.Symbol = symbol
	result.WindowMetrics = make(map[string][]*dto.MetricDTO)

	for _, row := range rows {
		windowId := row.WindowId
		if result.WindowMetrics[windowId] == nil {
			result.WindowMetrics[windowId] = make([]*dto.MetricDTO, 0)
		}

		// for all metrics which are there, add the entry
		for _, metric := range metrics {
			metricDto := &dto.MetricDTO{
				Window:  row.WindowId,
				StartTs: row.StartTs,
				EndTs:   row.EndTs,
				Name:    metric,
			}

			switch metric {
			case "volume":
				metricDto.Value = row.Volume
			case "rolling_volume":
				metricDto.Value = row.RollingVolume
			case "volume_acceleration":
				metricDto.Value = row.VolumeAcceleration
			case "volatility":
				metricDto.Value = row.Volatility
			case "atr":
				metricDto.Value = row.Atr
			case "ema":
				metricDto.Value = row.Ema
			case "sma":
				metricDto.Value = row.Sma
			case "log_return":
				metricDto.Value = row.LogReturn
			case "simple_return":
				metricDto.Value = row.SimpleReturn
			case "twap":
				metricDto.Value = row.TWAP
			case "vwap":
				metricDto.Value = row.VWAP
			case "rolling_vwap":
				metricDto.Value = row.RollingVWAP
			default:
				logger.Log.Info("invalid input metric", "metrics", metrics)
				return nil, errors.New("Invalid metric")
			}

			result.WindowMetrics[windowId] = append(result.WindowMetrics[windowId], metricDto)
		}
	}

	return result, nil
}

func (s *MarketService) GetOrderbook(ctx context.Context, exchange, symbol string, depth int) (*dto.OrderbookDTO, error) {

	var res dto.OrderbookDTO
	row, err := s.repository.GetLatestBook(ctx, exchange, symbol, depth)
	if err != nil {
		return nil, err
	}

	newLevels := map[string][]*dto.OrderbookLevelDTO{}

	for side, levels := range row.Levels {
		if newLevels[side] == nil {
			newLevels[side] = make([]*dto.OrderbookLevelDTO, 0)
		}

		for _, level := range levels {
			newLevels[side] = append(newLevels[side], &dto.OrderbookLevelDTO{
				LevelIndex: level.LevelIndex,
				Price:      level.Price,
				Volume:     level.Volume,
			})
		}
	}

	res = dto.OrderbookDTO{
		Exchange:      row.Exchange,
		Symbol:        row.Symbol,
		EventTime:     row.EventTime,
		BestBidPrice:  row.BestBidPrice,
		BestAskPrice:  row.BestAskPrice,
		BestAskVolume: row.BestAskVolume,
		BestBidVolume: row.BestBidVolume,
		Spread:        row.Spread,
		Levels:        newLevels,
	}

	return &res, nil
}
