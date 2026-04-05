CREATE TABLE IF NOT EXISTS orderbook_flushes (
	exchange TEXT NOT NULL,
	symbol TEXT NOT NULL,
	event_time_millis BIGINT NOT NULL,
	event_time TIMESTAMPTZ NOT NULL,

	best_bid_price DOUBLE PRECISION,
	best_bid_volume DOUBLE PRECISION,
	best_ask_price DOUBLE PRECISION,
	best_ask_volume DOUBLE PRECISION,
	spread DOUBLE PRECISION,

	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	PRIMARY KEY(exchange, symbol, event_time)
) PARTITION BY RANGE (event_time);

-- will join this table to flushes table 
-- my foreign key here references PK of flushes table and this PK is FK + side, level
-- and create an index on FK to find all the side, levels for a snapshot quick

CREATE TABLE IF NOT EXISTS orderbook_flush_levels(
	exchange TEXT NOT NULL,
	symbol TEXT NOT NULL,
	event_time TIMESTAMPTZ NOT NULL,
	level_index INT NOT NULL,
	side CHAR(1) NOT NULL CHECK (side in ('B', 'A')),
	price DOUBLE PRECISION NOT NULL,
	volume DOUBLE PRECISION NOT NULL,
	PRIMARY KEY(exchange, symbol, event_time, side, level_index),
	FOREIGN KEY(exchange, symbol, event_time) REFERENCES orderbook_flushes(exchange, symbol, event_time) 
	ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_flushes_symbol_time ON orderbook_flush_levels (exchange, symbol, event_time DESC);
