export const ALL_METRICS = [
    "microprice",
    "volume",
    "rolling_volume",
    "volume_acceleration",
    "volatility",
    "atr",
    "ema",
    "sma",
    "log_return",
    "simple_return",
    "twap",
    "vwap",
    "rolling_vwap"
] as const;

export type MetricName = (typeof ALL_METRICS)[number]

export interface CandleDTO {
    start_ts: string;
    end_ts: string;
    open: number;
    low: number;
    high: number;
    close: number;
    volume: number;
}

export interface MetricDTO {
    window: string;
    name: MetricName;
    value: number;
    start_ts: string;
    end_ts: string;
}

export interface MetricResultDTO {
    exchange: string;
    symbol: string;
    window_metrics: Record<string, MetricDTO[]>;
}

export interface OrderbookLevelDTO {
    level_index: number;
    price: number;
    volume: number;
}

export interface OrderbookDTO {
    exchange: string;
    symbol: string;
    event_time: string;
    best_bid_price: number;
    best_bid_volume: number;
    best_ask_price: number;
    best_ask_volume: number;
    spread: number;
    levels: Record<string, OrderbookLevelDTO[]>;
}

export interface TickStreamMessage {
    exchange: string;
    symbol: string;
    window_id: string;
    start_ts_ms: number;
    end_ts_ms: number;
    price_metrics?: {
        ohlc?: {
            open: number;
            low: number;
            high: number;
            close: number;
        };
        vwap?: number;
        rolling_vwap?: number;
        twap?: number;
        microprice?: number;
    };
    volume_metrics?: {
        volume?: number;
        rolling_volume?: number;
        volume_acceleration?: number;
    };
    volatility_metrics?: {
        volatility?: number;
        atr?: number;
    };
    trend_metrics?: {
        ema?: number;
        sma?: number;
        log_return?: number;
        simple_return?: number;
    }
}

export interface BookLevelStream {
    price: number;
    volume: number;
}

export interface OrderbookStreamMessage {
    exchange: string;
    symbol: string;
    event_time_millis?: number;
    event_time?: string;
    bids: BookLevelStream[];
    asks: BookLevelStream[];
    bestBid?: BookLevelStream;
    bestAsk?: BookLevelStream;
    spread?: number;
}

export type WSSubscriptionType = "tick" | "book"
