package aggmetrics

import (
	"market-aggregator/constants"
	"market-aggregator/proto/generated"
	"math"
	"shared/logger"
)

// its a decayed rolling metric where old values still contribute to the current value but its importance reduces with age
// use ewma half life formula to compute alpha
// e.g if window is 24h, its value now should take in to account 50% of the value 24h back

// value of last 24 hours = 50% = 50/100 = 0.5
// N updates occurred in the last 24 hours - means N ticks updated the window
// ewma formula - value which occurred N updates earlier contributes value (1-a)^N
// a is alpha value here
// (1-a)^N = 0.5
// N ln(1-a) = ln(0.5)
// ln (1-a) = ln(0.5)/N
// 1-a = e ^ (ln(0.5)/N)
// a = 1 - e ^ (ln(0.5)/N)

// estimating 1 tick arrives per second for a symbol at a specific exchange and specific channel

type EMA struct {
	Alpha float64
	Value float64
	Init  bool
}

func NewEMA(cfg *constants.WindowConfig) *EMA {
	numUpdates := cfg.DurationMs / 1000 // number of seconds as 1 tick/sec estimation
	alpha := 1 - math.Exp(math.Log(0.5)/float64(numUpdates))
	return &EMA{
		Alpha: alpha,
	}
}

func (e *EMA) Update(t *generated.NormalizedTick) {
	if !e.Init {
		// first value always has to be first observation. no alpha involved yet
		e.Value = t.Price
		e.Init = true
	} else {
		e.Value = e.Alpha*t.Price + (1-e.Alpha)*e.Value
	}
	logger.Log.Info("Updating EMA", "value", e.Value, "exchange", t.Exchange, "symbol", t.Symbol, "event_time", t.EventTsMillis)

}

func (e *EMA) Apply(a *generated.AggregatedTick) {
	if a.TrendMetrics == nil {
		a.TrendMetrics = &generated.TrendMetrics{}
	}

	a.TrendMetrics.Ema = e.Value
}

// no-op as decayed rolling
func (e *EMA) Reset() {}

func (e *EMA) GetValue() float64 {
	return e.Value
}
