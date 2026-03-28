import { create } from "zustand";
import {ALL_METRICS, type MetricName} from "@/types/types";

type MetricSelection = Record<MetricName, boolean>;
type ThemeMode = "light" | "dark";

interface UIState {
    exchange: string;
    symbol: string;
    windowId: string;
    depth: number;
    theme: ThemeMode;
    metricSelection: MetricSelection;
    setExchange: (exchange: string) => void;
    setSymbol: (symbol: string) => void;
    setWindowId: (windowId: string) => void;
    setPair: (exchange: string, symbol: string) => void;
    setDepth: (depth: number) => void;
    setTheme: (theme: ThemeMode) => void;
    toggleTheme: () => void;
    setMetricEnabled: (metric: MetricName, enabled: boolean) => void;
    toggleMetric: (metric: MetricName) => void;
    selectedMetrics: () => MetricName[];
}

function buildDefaultMetricSelection(): MetricSelection {
    const enabled = new Set<MetricName>(["ema", "vwap", "volume", "volatility"])
    return ALL_METRICS.reduce((acc, metric) => {
        acc[metric] = enabled.has(metric)
        return acc
    }, {} as MetricSelection)

}

// lightweight ui store for metrics
export const useUIStore = create<UIState>((set, get) => ({
    exchange: "binance",
    symbol: "BTCUSDT",
    windowId: "1m",
    depth: 10,
    theme: "dark",
    metricSelection: buildDefaultMetricSelection(),
    setExchange: (exchange) => set({ exchange }),
    setSymbol: (symbol) => set({ symbol }),
    setWindowId: (windowId) => set({ windowId }),
    setPair: (exchange, symbol) => set({ exchange, symbol }),
    setDepth: (depth) => set({depth}),
    setTheme: (theme) => set({ theme }),
    toggleTheme: () => set((state) => ({ theme: state.theme === "dark" ? "light" : "dark" })),
    selectedMetrics: () => {
        const selection = get().metricSelection;
        return ALL_METRICS.filter((m) => selection[m])
    },
    setMetricEnabled: (metric, enabled) => set((state) => ({
        metricSelection: {
            ...state.metricSelection,
            [metric]: enabled,
        }
    })),
    toggleMetric: (metric) => set((state) => ({
        metricSelection: {
            ...state.metricSelection,
            [metric]: !state.metricSelection[metric],
        }
    }))

}));
