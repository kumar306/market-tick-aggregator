package service

import (
	"context"
	"market-ui-backend/dto"
	"market-ui-backend/repository"
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
