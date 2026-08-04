package aggmetrics

import "encoding/gob"

// for aggregator state checkpointing purpose - huge windows state is lost on crash before flush if didnt checkpoint
// register every concrete metric type so gob can encode/decode them through
// the constants.Metric interface (map values in a window's Metrics map).
func init() {
	gob.Register(&OHLC{})
	gob.Register(&EMA{})
	gob.Register(&VWAP{})
	gob.Register(&TWAP{})
	gob.Register(&Returns{})
	gob.Register(&Volume{})
	gob.Register(&RollingVolume{})
	gob.Register(&RollingVWAP{})
	gob.Register(&Volatility{})
	gob.Register(&ATR{})
}
