import { create } from "zustand";
import { MetricName, OrderbookStreamMessage, TickStreamMessage } from "@/types/types";

export type WSStatus = "idle" | "connecting" | "open" | "closed";

export interface CandleBar {
  open: number;
  close: number;
  low: number;
  high: number;
  volume: number;
  timeSec: number;
}

export interface MetricPoint {
  value: number;
  timeSec: number;
}

const MAX_BARS_PER_SERIES = 1500;
const EMPTY_CANDLES: CandleBar[] = [];
const EMPTY_POINTS: MetricPoint[] = [];

interface MarketStreamState {
  wsState: WSStatus;
  ticksByKey: Record<string, TickStreamMessage>;
  booksByKey: Record<string, OrderbookStreamMessage>;
  candleSeriesByKey: Record<string, CandleBar[]>;
  metricSeriesByKey: Record<string, MetricPoint[]>;
  setWsStatus: (status: WSStatus) => void;
  upsertTick: (tick: TickStreamMessage) => void;
  upsertBook: (book: OrderbookStreamMessage) => void;
  seedCandleSeries: (seriesKey: string, candles: CandleBar[]) => void;
  seedMetricSeries: (seriesKey: string, points: MetricPoint[]) => void;
  clearKey: (key: string) => void;
}

export function buildSeriesKey(exchange: string, symbol: string, windowId: string): string {
  return `${exchange}:${symbol}:${windowId}`;
}

export function buildMetricSeriesKey(exchange: string, symbol: string, windowId: string, metric: MetricName): string {
  return `${exchange}:${symbol}:${windowId}:${metric}`;
}

function toCandleBarFromTick(tick: TickStreamMessage): CandleBar | null {
  const ohlc = tick.price_metrics?.ohlc;
  if (!ohlc) return null;

  return {
    close: ohlc.close,
    high: ohlc.high,
    low: ohlc.low,
    open: ohlc.open,
    volume: tick.volume_metrics?.volume ?? 0,
    timeSec: Math.floor(tick.end_ts_ms / 1000),
  };
}

function upsertBar(series: CandleBar[], next: CandleBar): CandleBar[] {
  if (series.length === 0) return [next];

  const last = series[series.length - 1];

  if (next.timeSec > last.timeSec) {
    const appended = [...series, next];
    return appended.length > MAX_BARS_PER_SERIES ? appended.slice(appended.length - MAX_BARS_PER_SERIES) : appended;
  }

  if (next.timeSec === last.timeSec) {
    if (candleEquals(last, next)) return series;
    const copy = [...series];
    copy[copy.length - 1] = next;
    return copy;
  }

  // out of order case, replace if same bucket exists else ignore it
  const idx = series.findIndex((bar) => bar.timeSec === next.timeSec);
  if (idx < 0) return series;

  if (candleEquals(series[idx], next)) return series;
  const copy = [...series];
  copy[idx] = next;
  return copy;
}

function upsertPoint(series: MetricPoint[], next: MetricPoint): MetricPoint[] {
  if (series.length === 0) return [next];

  const last = series[series.length - 1];

  if (next.timeSec > last.timeSec) {
    const appended = [...series, next];
    return appended.length > MAX_BARS_PER_SERIES ? appended.slice(appended.length - MAX_BARS_PER_SERIES) : appended;
  }

  if (next.timeSec === last.timeSec) {
    if (pointEquals(last, next)) return series;
    const copy = [...series];
    copy[copy.length - 1] = next;
    return copy;
  }

  const idx = series.findIndex((point) => point.timeSec === next.timeSec);
  if (idx < 0) return series;
  if (pointEquals(series[idx], next)) return series;

  const copy = [...series];
  copy[idx] = next;
  return copy;
}

function metricPairsFromTick(tick: TickStreamMessage): Array<{ metric: MetricName; value: number }> {
  const pairs: Array<{ metric: MetricName; value: number | undefined }> = [
    { metric: "microprice", value: tick.price_metrics?.microprice },
    { metric: "volume", value: tick.volume_metrics?.volume },
    { metric: "rolling_volume", value: tick.volume_metrics?.rolling_volume },
    { metric: "volume_acceleration", value: tick.volume_metrics?.volume_acceleration },
    { metric: "volatility", value: tick.volatility_metrics?.volatility },
    { metric: "atr", value: tick.volatility_metrics?.atr },
    { metric: "ema", value: tick.trend_metrics?.ema },
    { metric: "sma", value: tick.trend_metrics?.sma },
    { metric: "log_return", value: tick.trend_metrics?.log_return },
    { metric: "simple_return", value: tick.trend_metrics?.simple_return },
    { metric: "twap", value: tick.price_metrics?.twap },
    { metric: "vwap", value: tick.price_metrics?.vwap },
    { metric: "rolling_vwap", value: tick.price_metrics?.rolling_vwap },
  ];

  return pairs.filter((pair): pair is { metric: MetricName; value: number } => Number.isFinite(pair.value));
}

function mergeAndTrimCandles(existing: CandleBar[], incoming: CandleBar[]): CandleBar[] {
  if (incoming.length === 0) return existing;

  // incoming = REST seed. existing includes newer websocket updates that should win on overlap.
  const byTime = new Map<number, CandleBar>();
  for (const bar of incoming) byTime.set(bar.timeSec, bar);
  for (const bar of existing) byTime.set(bar.timeSec, bar);

  const merged = Array.from(byTime.values()).sort((a, b) => a.timeSec - b.timeSec);
  const trimmed = merged.length > MAX_BARS_PER_SERIES ? merged.slice(merged.length - MAX_BARS_PER_SERIES) : merged;

  return isSameCandleSeries(existing, trimmed) ? existing : trimmed;
}

function mergeAndTrimPoints(existing: MetricPoint[], incoming: MetricPoint[]): MetricPoint[] {
  if (incoming.length === 0) return existing;

  const byTime = new Map<number, MetricPoint>();
  for (const point of incoming) byTime.set(point.timeSec, point);
  for (const point of existing) byTime.set(point.timeSec, point);

  const merged = Array.from(byTime.values()).sort((a, b) => a.timeSec - b.timeSec);
  const trimmed = merged.length > MAX_BARS_PER_SERIES ? merged.slice(merged.length - MAX_BARS_PER_SERIES) : merged;

  return isSamePointSeries(existing, trimmed) ? existing : trimmed;
}

function isSameCandleSeries(existing: CandleBar[], incoming: CandleBar[]): boolean {
  if (existing === incoming) return true;
  if (existing.length !== incoming.length) return false;

  for (let i = 0; i < existing.length; i++) {
    if (!candleEquals(existing[i], incoming[i])) return false;
  }

  return true;
}

function isSamePointSeries(existing: MetricPoint[], incoming: MetricPoint[]): boolean {
  if (existing === incoming) return true;
  if (existing.length !== incoming.length) return false;

  for (let i = 0; i < existing.length; i++) {
    if (!pointEquals(existing[i], incoming[i])) return false;
  }

  return true;
}

function candleEquals(a: CandleBar, b: CandleBar): boolean {
  return a.close === b.close && a.high === b.high && a.low === b.low && a.open === b.open && a.volume === b.volume && a.timeSec === b.timeSec;
}

function pointEquals(a: MetricPoint, b: MetricPoint): boolean {
  return a.timeSec === b.timeSec && a.value === b.value;
}

function eventTimeMillis(book: OrderbookStreamMessage): number | null {
  const millis = book.event_time_millis;
  if (typeof millis === "number" && Number.isFinite(millis)) return millis;
  if (book.event_time) {
    const parsed = Date.parse(book.event_time);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export const useMarketStore = create<MarketStreamState>((set) => ({
  wsState: "idle",
  ticksByKey: {},
  booksByKey: {},
  candleSeriesByKey: {},
  metricSeriesByKey: {},
  setWsStatus: (status) => set({ wsState: status }),
  upsertTick: (tick) =>
    set((state) => {
      const key = `${tick.exchange}:${tick.symbol}`;
      const prev = state.ticksByKey[key];
      const tickChanged = !prev || prev.end_ts_ms !== tick.end_ts_ms || prev.start_ts_ms !== tick.start_ts_ms || prev.window_id !== tick.window_id;

      const patch: Partial<MarketStreamState> = {};

      if (tickChanged) {
        patch.ticksByKey = {
          ...state.ticksByKey,
          [key]: tick,
        };
      }

      // extract the candlebar object from tick, get the series key, add the candlebar to existing series and set to state
      const nextBar = toCandleBarFromTick(tick);
      if (nextBar) {
        const seriesKey = buildSeriesKey(tick.exchange, tick.symbol, tick.window_id);
        const prevSeries = state.candleSeriesByKey[seriesKey] ?? EMPTY_CANDLES;
        const nextSeries = upsertBar(prevSeries, nextBar);

        if (nextSeries !== prevSeries) {
          patch.candleSeriesByKey = {
            ...state.candleSeriesByKey,
            [seriesKey]: nextSeries,
          };
        }
      }

      // extract metrics from tick. for each metric, get its key, append to existing series and set to state
      const metricPairs = metricPairsFromTick(tick);
      if (metricPairs.length > 0) {
        const baseTimeSec = Math.floor(tick.end_ts_ms / 1000);
        let nextMetricSeriesByKey: Record<string, MetricPoint[]> | undefined;

        for (const pair of metricPairs) {
          const metricSeriesKey = buildMetricSeriesKey(tick.exchange, tick.symbol, tick.window_id, pair.metric);
          const prevSeries = (nextMetricSeriesByKey ?? state.metricSeriesByKey)[metricSeriesKey] ?? EMPTY_POINTS;
          const nextSeries = upsertPoint(prevSeries, { timeSec: baseTimeSec, value: pair.value });
          if (nextSeries !== prevSeries) {
            if (!nextMetricSeriesByKey) nextMetricSeriesByKey = { ...state.metricSeriesByKey };
            nextMetricSeriesByKey[metricSeriesKey] = nextSeries;
          }
        };
        if (nextMetricSeriesByKey) patch.metricSeriesByKey = nextMetricSeriesByKey;
      }

      if (!patch.ticksByKey && !patch.candleSeriesByKey && !patch.metricSeriesByKey) return state;
      return patch;
    }),
  upsertBook: (book) =>
    set((state) => {
      const key = `${book.exchange}:${book.symbol}`;
      const prev = state.booksByKey[key];
      if (prev) {
        const prevTime = eventTimeMillis(prev);
        const nextTime = eventTimeMillis(book);
        if (prevTime && nextTime && prevTime === nextTime) {
          return state;
        }
      }
      return {
        booksByKey: {
          ...state.booksByKey,
          [key]: book,
        },
      };
    }),
  clearKey: (key) =>
    set((state) => {
      const nextTicks = { ...state.ticksByKey };
      const nextBooks = { ...state.booksByKey };
      const nextCandles = { ...state.candleSeriesByKey };
      const nextMetrics = { ...state.metricSeriesByKey };
      delete nextTicks[key];
      delete nextBooks[key];

      const prefix = `${key}:`;
      for (const seriesKey of Object.keys(nextCandles)) {
        if (seriesKey.startsWith(prefix)) {
          delete nextCandles[seriesKey];
        }
      }
      for (const metricKey of Object.keys(nextMetrics)) {
        if (metricKey.startsWith(prefix)) {
          delete nextMetrics[metricKey];
        }
      }

      return {
        ticksByKey: nextTicks,
        booksByKey: nextBooks,
        candleSeriesByKey: nextCandles,
        metricSeriesByKey: nextMetrics,
      };
    }),
  seedCandleSeries: (seriesKey, candles) =>
    set((state) => {
      const existing = state.candleSeriesByKey[seriesKey] ?? EMPTY_CANDLES;
      const merged = mergeAndTrimCandles(existing, candles);
      if (existing === merged) return state;

      return {
        candleSeriesByKey: {
          ...state.candleSeriesByKey,
          [seriesKey]: merged,
        },
      };
    }),
  seedMetricSeries: (seriesKey, points) =>
    set((state) => {
      const existing = state.metricSeriesByKey[seriesKey] ?? EMPTY_POINTS;
      // merging instead of overwriting as we may get the rest seed after websocket update happened
      // to ensure historical rest slice is present along with received live updates and trim to the max number of points
      // dont want to throw away live updates, rather merge it
      const merged = mergeAndTrimPoints(existing, points);
      if (existing === merged) return state;

      return {
        metricSeriesByKey: {
          ...state.metricSeriesByKey,
          [seriesKey]: merged,
        },
      };
    }),
}));

export const selectTickByKey = (key: string) => (state: MarketStreamState): TickStreamMessage | undefined => state.ticksByKey[key];
export const selectBookByKey = (key: string) => (state: MarketStreamState): OrderbookStreamMessage | undefined => state.booksByKey[key];
export const selectCandleSeriesByKey = (seriesKey: string) => (state: MarketStreamState): CandleBar[] => state.candleSeriesByKey[seriesKey] ?? EMPTY_CANDLES;
export const selectMetricSeriesByKey = (seriesKey: string) => (state: MarketStreamState): MetricPoint[] => state.metricSeriesByKey[seriesKey] ?? EMPTY_POINTS;
