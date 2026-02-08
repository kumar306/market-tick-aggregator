CREATE TABLE aggregated_ticks(
	exchange TEXT NOT NULL,
	symbol TEXT NOT NULL,
	window_id TEXT NOT NULL,
	
	start_ts_ms BIGINT NOT NULL,
	end_ts_ms BIGINT NOT NULL,

	start_ts TIMESTAMPTZ NOT NULL,
	end_ts TIMESTAMPTZ NOT NULL,

	open_price DOUBLE PRECISION NOT NULL,
	close_price DOUBLE PRECISION NOT NULL,
	low_price DOUBLE PRECISION NOT NULL,
	high_price DOUBLE PRECISION NOT NULL,
	vwap DOUBLE PRECISION NOT NULL,
	rolling_vwap DOUBLE PRECISION NOT NULL,
	twap DOUBLE PRECISION NOT NULL,
	microprice DOUBLE PRECISION NOT NULL,

	volume DOUBLE PRECISION NOT NULL,
	rolling_volume DOUBLE PRECISION NOT NULL,
	volume_acceleration DOUBLE PRECISION NOT NULL,

	volatility DOUBLE PRECISION NOT NULL,
	atr DOUBLE PRECISION NOT NULL,

	ema DOUBLE PRECISION NOT NULL,
	sma DOUBLE PRECISION NOT NULL,
	log_return DOUBLE PRECISION NOT NULL,
	simple_return DOUBLE PRECISION NOT NULL,

	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

	PRIMARY KEY (exchange, symbol, window_id, start_ts)
) PARTITION BY RANGE (start_ts);

-- create some partitions in advance

CREATE TABLE aggregated_ticks_2026_02_07 
PARTITION OF aggregated_ticks 
FOR VALUES FROM ('2026-02-07 00:00:00+00') 
TO ('2026-02-08 00:00:00+00');

-- create secondary index
CREATE INDEX idx_agg_ticks_symbol_time ON aggregated_ticks (exchange, symbol, start_ts DESC);