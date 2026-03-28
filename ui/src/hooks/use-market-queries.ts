import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { getCandles, getMetrics, getOrderbook } from "@/services/http";
import { MetricName } from "@/types/types";


type DateInput = Date | string;

interface BaseParams {
    exchange: string;
    symbol: string;
    window?: string;
    from: DateInput;
    to: DateInput;
    enabled?: boolean;
}

interface MetricsParams extends BaseParams {
    windows: string[];
    metrics: MetricName[];
}

interface OrderbookParams {
    exchange: string;
    symbol: string;
    depth: number;
    enabled?: boolean;
}

export function useCandlesQuery(params: BaseParams) {
    const { exchange, symbol, from, to, enabled = true } = params;

    return useQuery({
        queryKey: ["candles", exchange, symbol, params.window ?? "", String(from), String(to)],
        queryFn: () => getCandles({ exchange, symbol, window: params.window ?? "1m", from, to }),
        enabled: enabled && Boolean(exchange) && Boolean(symbol),
        staleTime: 5_000,
    });
}

export function useMetricsQuery(params: MetricsParams) {
    const { exchange, symbol, from, to, windows, metrics, enabled = true } = params;
    const sortedWindows = useMemo(() => [...windows].sort(), [windows]);
    const sortedMetrics = useMemo(() => [...metrics].sort(), [metrics]);

    return useQuery({
        queryKey: ["metrics", 
            exchange, 
            symbol, 
            String(from), 
            String(to), 
            sortedWindows.join(","), 
            sortedMetrics.join(",")],
        queryFn: () => getMetrics({
            exchange, symbol, from, to, windows: sortedWindows, metrics: sortedMetrics
        }),
        enabled: enabled && Boolean(exchange) && Boolean(symbol) && sortedWindows.length > 0 && sortedMetrics.length > 0,
        staleTime: 5_000,
    })
}

export function useOrderbookQuery(params: OrderbookParams) {
    const { exchange, symbol, depth, enabled = true } = params;
    return useQuery({
        queryKey: ["orderbook", exchange, symbol, depth],
        queryFn: () => getOrderbook({ exchange, symbol, depth }),
        enabled: enabled && Boolean(exchange) && Boolean(symbol) && depth > 0,
        staleTime: 2_000,
        refetchInterval: 5_000,
    })
}
