"use client";

import { useEffect, useMemo, useState } from "react";
import { ChartPanel } from "@/components/chart-panel";
import { MetricSidebar } from "@/components/metric-sidebar";
import { OrderbookPanel } from "@/components/orderbook-panel";
import { TopBar } from "@/components/top-bar";
import { useCandlesQuery, useMetricsQuery, useOrderbookQuery } from "@/hooks/use-market-queries";
import { useMarketStream } from "@/hooks/use-market-stream";
import { useMarketStore } from "@/store/market-store";
import { useUIStore } from "@/store/ui-store";
import { ALL_METRICS } from "@/types/types";

function lookbackMs(windowId: string): number {
  switch (windowId) {
    case "5s":
      return 45 * 24 * 60 * 60 * 1000;
    case "10s":
      return 45 * 24 * 60 * 60 * 1000;
    case "30s":
      return 45 * 24 * 60 * 60 * 1000;
    case "1m":
      return 45 * 24 * 60 * 60 * 1000;
    case "2m":
      return 45 * 24 * 60 * 60 * 1000;
    case "5m":
      return 60 * 24 * 60 * 60 * 1000;
    case "10m":
      return 60 * 24 * 60 * 60 * 1000;
    case "30m":
      return 90 * 24 * 60 * 60 * 1000;
    case "1h":
      return 120 * 24 * 60 * 60 * 1000;
    default:
      return 45 * 24 * 60 * 60 * 1000;
  }
}

function refreshMs(windowId: string): number {
  switch (windowId) {
    case "1m":
      return 15_000;
    case "5m":
      return 30_000;
    case "15m":
      return 45_000;
    case "1h":
      return 60_000;
    default:
      return 30_000;
  }
}

function toErrorText(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}

export default function Page() {
  // open ws connection
  useMarketStream();

  const exchange = useUIStore((s) => s.exchange);
  const symbol = useUIStore((s) => s.symbol);
  const windowId = useUIStore((s) => s.windowId);
  const depth = useUIStore((s) => s.depth);
  const theme = useUIStore((s) => s.theme);
  const metricSelection = useUIStore((s) => s.metricSelection);
  const wsState = useMarketStore((s) => s.wsState);
  const [nowMs, setNowMs] = useState(() => Date.now());

  const selectedMetrics = useMemo(() => ALL_METRICS.filter((metric) => metricSelection[metric]), [metricSelection]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      setNowMs(Date.now());
    }, refreshMs(windowId));

    return () => window.clearInterval(timer);
  }, [exchange, symbol, windowId]);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark");
  }, [theme]);

  const range = useMemo(() => {
    const to = new Date(nowMs);
    const from = new Date(to.getTime() - lookbackMs(windowId));
    return { from, to };
  }, [nowMs, windowId]);

  const candlesQuery = useCandlesQuery({
    exchange,
    symbol,
    window: windowId,
    from: range.from,
    to: range.to,
  });

  const metricsQuery = useMetricsQuery({
    exchange,
    symbol,
    from: range.from,
    to: range.to,
    windows: [windowId],
    metrics: selectedMetrics,
    enabled: selectedMetrics.length > 0,
  });

  const orderbookQuery = useOrderbookQuery({
    exchange,
    symbol,
    depth,
  });

  const combinedError = candlesQuery.error ?? metricsQuery.error ?? orderbookQuery.error;
  const chartsLoading = candlesQuery.isLoading || candlesQuery.isFetching || (selectedMetrics.length > 0 && (metricsQuery.isLoading || metricsQuery.isFetching));

  return (
    <main className={`min-h-screen p-4 transition-colors md:p-6 ${
      theme === "dark"
        ? "bg-[radial-gradient(circle_at_top_right,_#172554_0%,_#0f172a_42%,_#020617_100%)]"
        : "bg-[radial-gradient(circle_at_top_right,_#dbeafe_0%,_#f8fafc_35%,_#f8fafc_100%)]"
    }`}>
      <div className="mx-auto max-w-[1600px] space-y-4">
        <TopBar wsState={wsState} />

        {combinedError ? (
          <section className={`rounded-2xl px-4 py-3 text-sm ${
            theme === "dark"
              ? "border border-rose-500/30 bg-rose-950/70 text-rose-200"
              : "border border-rose-200 bg-rose-50 text-rose-700"
          }`}>
            {toErrorText(combinedError)}
          </section>
        ) : null}

        <div className="grid gap-4 xl:grid-cols-[290px_minmax(0,1fr)]">
          <MetricSidebar />

          <section className="space-y-4">
            <ChartPanel
              key={`${exchange}:${symbol}:${windowId}`}
              exchange={exchange}
              symbol={symbol}
              windowId={windowId}
              selectedMetrics={selectedMetrics}
              restCandles={candlesQuery.data}
              restMetrics={metricsQuery.data}
              loading={chartsLoading}
            />

            <OrderbookPanel
              exchange={exchange}
              symbol={symbol}
              depth={depth}
              restBook={orderbookQuery.data}
              loading={orderbookQuery.isLoading || orderbookQuery.isFetching}
            />
          </section>
        </div>
      </div>
    </main>
  );
}
