"use client";

import { Dispatch, memo, MutableRefObject, SetStateAction, useEffect, useMemo, useRef, useState } from "react";
import {
  formatMetricValue,
  isOverlayMetric,
  metricColor,
  metricGroup,
  metricLabel,
  METRIC_GROUPS,
} from "@/lib/metric-config";
import { CandleDTO, MetricName, MetricResultDTO } from "@/types/types";
import {
  CandlestickData,
  CandlestickSeries,
  ColorType,
  createChart,
  LineData,
  LineSeries,
  LogicalRange,
  UTCTimestamp,
  type IChartApi,
  type ISeriesApi,
} from "lightweight-charts";
import {
  buildMetricSeriesKey,
  buildSeriesKey,
  selectCandleSeriesByKey,
  useMarketStore,
  type CandleBar,
  type MetricPoint,
} from "@/store/market-store";

interface ChartPanelProps {
  exchange: string;
  symbol: string;
  restCandles?: CandleDTO[];
  restMetrics?: MetricResultDTO;
  selectedMetrics: MetricName[];
  windowId: string;
  loading: boolean;
}

function dtoToBar(candle: CandleDTO): CandleBar | null {
  const timeMs = Date.parse(candle.end_ts);
  if (!Number.isFinite(timeMs)) return null;

  return {
    timeSec: Math.floor(timeMs / 1000),
    close: candle.close,
    high: candle.high,
    low: candle.low,
    open: candle.open,
    volume: candle.volume,
  };
}

// mapping from metric dto[] (backend format) to internal metricPoint[] format and grouping by metricName
function metricSeedsByName(result: MetricResultDTO | undefined, windowId: string): Partial<Record<MetricName, MetricPoint[]>> {
  const rows = result?.window_metrics?.[windowId] ?? [];
  const mapped: Partial<Record<MetricName, MetricPoint[]>> = {};
  const byMetric: Partial<Record<MetricName, Map<number, number>>> = {};

  for (const row of rows) {
    const timeMs = Date.parse(row.end_ts);
    if (!Number.isFinite(timeMs)) continue;

    if (!byMetric[row.name]) byMetric[row.name] = new Map<number, number>();
    byMetric[row.name]!.set(Math.floor(timeMs / 1000), row.value);
  }

  for (const [metric, values] of Object.entries(byMetric) as Array<[MetricName, Map<number, number>]>) {
    mapped[metric] = Array.from(values.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([timeSec, value]) => ({ timeSec, value }));
  }

  return mapped;
}

function logicalRangeEquals(a: LogicalRange | null, b: LogicalRange | null): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  return Math.abs(a.from - b.from) < 0.01 && Math.abs(a.to - b.to) < 0.01;
}

// i pass in the range and its dispatch fn
// 1 use effect to subscribe to change in chart range and set new state
// 1 use effect to actually set the range in chart timescale api
function useSyncedLogicalRange(
  chartRef: MutableRefObject<IChartApi | null>,
  sharedLogicalRange: LogicalRange | null,
  onLogicalRangeChange: Dispatch<SetStateAction<LogicalRange | null>>,
) {
  const applyingRangeRef = useRef(false);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;

    const timeScale = chart.timeScale();
    const handler = (range: LogicalRange | null) => {
      if (applyingRangeRef.current) return;
      onLogicalRangeChange((prev) => (logicalRangeEquals(prev, range) ? prev : range));
    };

    // event listener when the range changes to set the new range in component which is newly rendered
    timeScale.subscribeVisibleLogicalRangeChange(handler);
    return () => {
      timeScale.unsubscribeVisibleLogicalRangeChange(handler);
    };
  }, [chartRef, onLogicalRangeChange]);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !sharedLogicalRange) return;

    const timeScale = chart.timeScale();
    const currentRange = timeScale.getVisibleLogicalRange();
    if (logicalRangeEquals(currentRange, sharedLogicalRange)) return;

    applyingRangeRef.current = true;
    timeScale.setVisibleLogicalRange(sharedLogicalRange);
    const reset = window.setTimeout(() => {
      applyingRangeRef.current = false;
    }, 0);

    return () => window.clearTimeout(reset);
  }, [chartRef, sharedLogicalRange]);
}

// return a view of given exchage, symbol, window, metric - its points
function useMetricSeries(exchange: string, symbol: string, windowId: string, metrics: MetricName[]) {
  return useMarketStore((state) => {
    const series: Partial<Record<MetricName, MetricPoint[]>> = {};
    for (const metric of metrics) {
      const key = buildMetricSeriesKey(exchange, symbol, windowId, metric);
      series[metric] = state.metricSeriesByKey[key] ?? [];
    }
    return series;
  });
}

function MetricLegend({ metrics, seriesByMetric }: { metrics: MetricName[]; seriesByMetric: Partial<Record<MetricName, MetricPoint[]>> }) {
  if (metrics.length === 0) return null;

  return (
    <div className="flex flex-wrap gap-2">
      {metrics.map((metric) => {
        const latest = seriesByMetric[metric]?.[seriesByMetric[metric]!.length - 1];
        return (
          <div key={metric} className="flex items-center gap-2 rounded-full border border-slate-200 bg-white/80 px-2 py-1 text-[11px] text-slate-600">
            <span className="h-2 w-2 rounded-full" style={{ backgroundColor: metricColor(metric) }} />
            <span>{metricLabel(metric)}</span>
            <span className="font-medium text-slate-800">{formatMetricValue(metric, latest?.value)}</span>
          </div>
        );
      })}
    </div>
  );
}

function PriceChart({
  exchange,
  symbol,
  windowId,
  bars,
  overlayMetrics,
  overlaySeriesByMetric,
  loading,
  sharedLogicalRange,
  onLogicalRangeChange,
}: {
  exchange: string;
  symbol: string;
  windowId: string;
  bars: CandleBar[];
  overlayMetrics: MetricName[];
  overlaySeriesByMetric: Partial<Record<MetricName, MetricPoint[]>>;
  loading: boolean;
  sharedLogicalRange: LogicalRange | null;
  onLogicalRangeChange: Dispatch<SetStateAction<LogicalRange | null>>;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const overlaySeriesRef = useRef<Partial<Record<MetricName, ISeriesApi<"Line">>>>({});
  const didFitRef = useRef(false);

  // hook to update state of zoom and pan
  useSyncedLogicalRange(chartRef, sharedLogicalRange, onLogicalRangeChange);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const chart = createChart(el, {
      layout: {
        textColor: "#1e293b",
        background: { type: ColorType.Solid, color: "#ffffff" },
      },
      grid: {
        vertLines: { color: "#e2e8f0" },
        horzLines: { color: "#f1f5f9" },
      },
      rightPriceScale: {
        borderColor: "#cbd5e1",
      },
      timeScale: {
        borderColor: "#cbd5e1",
        timeVisible: true,
        secondsVisible: false,
        lockVisibleTimeRangeOnResize: true,
      },
      crosshair: {
        vertLine: { color: "#64748b", width: 1 },
        horzLine: { color: "#94a3b8", width: 1 },
      },
      width: el.clientWidth,
      height: 420,
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#16a34a",
      borderUpColor: "#16a34a",
      wickUpColor: "#16a34a",
      downColor: "#dc2626",
      borderDownColor: "#dc2626",
      wickDownColor: "#dc2626",
      priceLineVisible: false,
      lastValueVisible: true,
    });

    const lineSeriesByMetric: Partial<Record<MetricName, ISeriesApi<"Line">>> = {};
    for (const metric of overlayMetrics) {
      lineSeriesByMetric[metric] = chart.addSeries(LineSeries, {
        color: metricColor(metric),
        lineWidth: metric === "microprice" ? 1 : 2,
        lineStyle: metric === "microprice" ? 1 : 0,
        priceLineVisible: false,
        lastValueVisible: true,
      });
    }

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    overlaySeriesRef.current = lineSeriesByMetric;

    const resizeObserver = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) return;
      chart.applyOptions({
        width: Math.max(320, Math.floor(entry.contentRect.width)),
      });
    });

    resizeObserver.observe(el);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      overlaySeriesRef.current = {};
    };
  }, [overlayMetrics]);

  const candleData = useMemo<CandlestickData<UTCTimestamp>[]>(() => {
    return bars.map((bar) => ({
      time: bar.timeSec as UTCTimestamp,
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
    }));
  }, [bars]);

  const hasRenderableCandles = useMemo(() => {
    return bars.some((bar) => bar.open !== 0 || bar.high !== 0 || bar.low !== 0 || bar.close !== 0);
  }, [bars]);

  useEffect(() => {
    const candleSeries = candleSeriesRef.current;
    const chart = chartRef.current;
    if (!candleSeries || !chart) return;

    candleSeries.setData(hasRenderableCandles ? candleData : []);
    if (!didFitRef.current && (candleData.length > 0 || overlayMetrics.length > 0) && !sharedLogicalRange) {
      chart.timeScale().fitContent();
      didFitRef.current = true;
    }
  }, [candleData, hasRenderableCandles, overlayMetrics.length, sharedLogicalRange]);

  useEffect(() => {
    for (const metric of overlayMetrics) {
      const series = overlaySeriesRef.current[metric];
      if (!series) continue;

      const lineData: LineData<UTCTimestamp>[] = (overlaySeriesByMetric[metric] ?? []).map((point) => ({
        time: point.timeSec as UTCTimestamp,
        value: point.value,
      }));
      series.setData(lineData);
    }
  }, [overlayMetrics, overlaySeriesByMetric]);

  const latest = hasRenderableCandles ? bars[bars.length - 1] : undefined;

  return (
    <article className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="mb-3 flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold text-slate-800">
            Price ({exchange}:{symbol} | {windowId})
          </h2>
          <p className="mt-1 text-xs text-slate-500">Candles with price-derived overlays on a shared time axis.</p>
        </div>
        <div className="text-xs text-slate-500">{loading ? "loading..." : `bars: ${bars.length}`}</div>
      </div>

      <div className="mb-3 flex flex-wrap gap-4 text-xs text-slate-600">
        <span>O: {latest?.open?.toFixed(4) ?? "-"}</span>
        <span>H: {latest?.high?.toFixed(4) ?? "-"}</span>
        <span>L: {latest?.low?.toFixed(4) ?? "-"}</span>
        <span>C: {latest?.close?.toFixed(4) ?? "-"}</span>
      </div>

      {!hasRenderableCandles ? (
        <div className="rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
          Historical OHLC candles are zero in the current DB seed. Price overlays remain visible, and live websocket updates will render as soon as non-zero OHLC arrives.
        </div>
      ) : null}

      <MetricLegend metrics={overlayMetrics} seriesByMetric={overlaySeriesByMetric} />
      <div ref={containerRef} className="mt-3 h-[420px] w-full rounded-xl border border-slate-200" />
    </article>
  );
}

function GroupMetricChart({
  title,
  description,
  metrics,
  seriesByMetric,
  loading,
  sharedLogicalRange,
  onLogicalRangeChange,
}: {
  title: string;
  description: string;
  metrics: MetricName[];
  seriesByMetric: Partial<Record<MetricName, MetricPoint[]>>;
  loading: boolean;
  sharedLogicalRange: LogicalRange | null;
  onLogicalRangeChange: Dispatch<SetStateAction<LogicalRange | null>>;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<Partial<Record<MetricName, ISeriesApi<"Line">>>>({});
  const didFitRef = useRef(false);

  useSyncedLogicalRange(chartRef, sharedLogicalRange, onLogicalRangeChange);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const chart = createChart(el, {
      layout: {
        textColor: "#334155",
        background: { type: ColorType.Solid, color: "#ffffff" },
      },
      grid: {
        vertLines: { color: "#e2e8f0" },
        horzLines: { color: "#f1f5f9" },
      },
      rightPriceScale: {
        borderColor: "#cbd5e1",
      },
      timeScale: {
        borderColor: "#cbd5e1",
        timeVisible: true,
        secondsVisible: false,
        lockVisibleTimeRangeOnResize: true,
      },
      width: el.clientWidth,
      height: 240,
    });

    const lineSeriesByMetric: Partial<Record<MetricName, ISeriesApi<"Line">>> = {};
    for (const metric of metrics) {
      lineSeriesByMetric[metric] = chart.addSeries(LineSeries, {
        color: metricColor(metric),
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: true,
      });
    }

    chartRef.current = chart;
    seriesRef.current = lineSeriesByMetric;

    const resizeObserver = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) return;
      chart.applyOptions({
        width: Math.max(280, Math.floor(entry.contentRect.width)),
      });
    });

    resizeObserver.observe(el);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = {};
    };
  }, [metrics]);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;

    let hasData = false;
    for (const metric of metrics) {
      const series = seriesRef.current[metric];
      if (!series) continue;

      const lineData: LineData<UTCTimestamp>[] = (seriesByMetric[metric] ?? []).map((point) => ({
        time: point.timeSec as UTCTimestamp,
        value: point.value,
      }));

      if (lineData.length > 0) hasData = true;
      series.setData(lineData);
    }

    if (!didFitRef.current && hasData && !sharedLogicalRange) {
      chart.timeScale().fitContent();
      didFitRef.current = true;
    }
  }, [metrics, seriesByMetric, sharedLogicalRange]);

  return (
    <article className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="mb-3 flex items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold text-slate-800">{title}</h3>
          <p className="mt-1 text-xs text-slate-500">{description}</p>
        </div>
        <div className="text-xs text-slate-500">{loading ? "loading..." : `${metrics.length} metrics`}</div>
      </div>

      <MetricLegend metrics={metrics} seriesByMetric={seriesByMetric} />
      <div ref={containerRef} className="mt-3 h-[240px] w-full rounded-xl border border-slate-200" />
    </article>
  );
}

export const ChartPanel = memo(function ChartPanel({
  exchange,
  symbol,
  windowId,
  restCandles,
  restMetrics,
  selectedMetrics,
  loading,
}: ChartPanelProps) {
  const [sharedLogicalRange, setSharedLogicalRange] = useState<LogicalRange | null>(null);

  const seriesKey = useMemo(() => buildSeriesKey(exchange, symbol, windowId), [exchange, symbol, windowId]);
  const seriesSelector = useMemo(() => selectCandleSeriesByKey(seriesKey), [seriesKey]);
  const bars = useMarketStore(seriesSelector);
  const seedCandleSeries = useMarketStore((state) => state.seedCandleSeries);
  const seedMetricSeries = useMarketStore((state) => state.seedMetricSeries);

  // convering the rest candles response from CandleDto[] to Candlebar[] which is my internal candle format
  const seededBars = useMemo(() => {
    const mapped = (restCandles ?? []).map(dtoToBar).filter(Boolean) as CandleBar[];
    mapped.sort((a, b) => a.timeSec - b.timeSec);
    return mapped;
  }, [restCandles]);

  // convert to internal mapping for candles and metrics
  const metricSeeds = useMemo(() => metricSeedsByName(restMetrics, windowId), [restMetrics, windowId]);

  // when i get rest candles, i will merge it with existing series if i have
  useEffect(() => {
    if (seededBars.length === 0) return;
    seedCandleSeries(seriesKey, seededBars);
  }, [seedCandleSeries, seededBars, seriesKey]);

  // similarly merge rest metrics - by metric - each window metric has array of metricPoint[]. so merge those if exists
  useEffect(() => {
    for (const metric of selectedMetrics) {
      const points = metricSeeds[metric] ?? [];
      if (points.length === 0) continue;
      seedMetricSeries(buildMetricSeriesKey(exchange, symbol, windowId, metric), points);
    }
  }, [exchange, metricSeeds, seedMetricSeries, selectedMetrics, symbol, windowId]);

  const overlayMetrics = selectedMetrics.filter(isOverlayMetric);
  const panelGroups = METRIC_GROUPS.filter((group) => group.id !== "price")
    .map((group) => ({
      ...group,
      metrics: selectedMetrics.filter((metric) => metricGroup(metric) === group.id),
    }))
    .filter((group) => group.metrics.length > 0);

  // get a view of the overlay metrics for particular window combo
  const overlaySeriesByMetric = useMetricSeries(exchange, symbol, windowId, overlayMetrics);
  const panelMetrics = panelGroups.flatMap((group) => group.metrics);
  
  // get a view of the panel metrics - which are selected
  const panelSeriesByMetric = useMetricSeries(exchange, symbol, windowId, panelMetrics);

  // pass in logical range to price chart and each of the group charts
  return (
    <section className="space-y-4">
      <PriceChart
        key={`price:${overlayMetrics.join(",")}`}
        exchange={exchange}
        symbol={symbol}
        windowId={windowId}
        bars={bars}
        overlayMetrics={overlayMetrics}
        overlaySeriesByMetric={overlaySeriesByMetric}
        loading={loading}
        sharedLogicalRange={sharedLogicalRange}
        onLogicalRangeChange={setSharedLogicalRange}
      />

      {panelGroups.length > 0 ? (
        <div className="grid gap-4 xl:grid-cols-2">
          {panelGroups.map((group) => {
            const groupSeriesByMetric: Partial<Record<MetricName, MetricPoint[]>> = {};
            for (const metric of group.metrics) {
              groupSeriesByMetric[metric] = panelSeriesByMetric[metric] ?? [];
            }

            return (
              <GroupMetricChart
                key={`${group.id}:${group.metrics.join(",")}`}
                title={group.label}
                description={group.description}
                metrics={group.metrics}
                seriesByMetric={groupSeriesByMetric}
                loading={loading}
                sharedLogicalRange={sharedLogicalRange}
                onLogicalRangeChange={setSharedLogicalRange}
              />
            );
          })}
        </div>
      ) : (
        <article className="rounded-2xl border border-dashed border-slate-300 bg-white p-6 text-center text-sm text-slate-500">
          Select at least one non-price metric to render the lower observability panels.
        </article>
      )}
    </section>
  );
});
