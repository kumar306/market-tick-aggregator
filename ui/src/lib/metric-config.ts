import { MetricName } from "@/types/types";

export type MetricGroupId = "price" | "volume" | "flow" | "risk" | "returns";

type MetricFormat = "price" | "volume" | "percent" | "number";

interface MetricMeta {
  label: string;
  color: string;
  group: MetricGroupId;
  format: MetricFormat;
  overlay: boolean;
  description: string;
}

interface MetricGroupMeta {
  id: MetricGroupId;
  label: string;
  description: string;
}

// group by metric families. overlap metrics: microprice, ema, sma, twap, vwap, rolling_vwap
// group metric by panels as they share different units. cannot plot all the metrics on a same chart
// metrics belonging to same family (price, volume, risk, flow) have similar units and thus, made decision to group and display them together
export const METRIC_META: Record<MetricName, MetricMeta> = {
  microprice: {
    label: "Microprice",
    color: "#0f766e",
    group: "price",
    format: "price",
    overlay: true,
    description: "Orderbook-weighted fair price estimate.",
  },
  volume: {
    label: "Volume",
    color: "#0369a1",
    group: "volume",
    format: "volume",
    overlay: false,
    description: "Traded volume inside the active window.",
  },
  rolling_volume: {
    label: "Rolling Volume",
    color: "#2563eb",
    group: "volume",
    format: "volume",
    overlay: false,
    description: "Smoothed rolling traded volume.",
  },
  volume_acceleration: {
    label: "Volume Acceleration",
    color: "#7c3aed",
    group: "flow",
    format: "number",
    overlay: false,
    description: "Rate of change of traded volume.",
  },
  volatility: {
    label: "Volatility",
    color: "#c2410c",
    group: "risk",
    format: "number",
    overlay: false,
    description: "Window volatility from tick updates.",
  },
  atr: {
    label: "ATR",
    color: "#ea580c",
    group: "risk",
    format: "price",
    overlay: false,
    description: "Average true range as a price-move proxy.",
  },
  ema: {
    label: "EMA",
    color: "#8b5cf6",
    group: "price",
    format: "price",
    overlay: true,
    description: "Exponential moving average.",
  },
  sma: {
    label: "SMA",
    color: "#a855f7",
    group: "price",
    format: "price",
    overlay: true,
    description: "Simple moving average.",
  },
  log_return: {
    label: "Log Return",
    color: "#db2777",
    group: "returns",
    format: "percent",
    overlay: false,
    description: "Accumulated log return for the window.",
  },
  simple_return: {
    label: "Simple Return",
    color: "#e11d48",
    group: "returns",
    format: "percent",
    overlay: false,
    description: "Accumulated simple return for the window.",
  },
  twap: {
    label: "TWAP",
    color: "#0284c7",
    group: "price",
    format: "price",
    overlay: true,
    description: "Time-weighted average price.",
  },
  vwap: {
    label: "VWAP",
    color: "#1d4ed8",
    group: "price",
    format: "price",
    overlay: true,
    description: "Volume-weighted average price.",
  },
  rolling_vwap: {
    label: "Rolling VWAP",
    color: "#4f46e5",
    group: "price",
    format: "price",
    overlay: true,
    description: "Rolling volume-weighted average price.",
  },
};

// metric families which i can group metrics in panels
export const METRIC_GROUPS: MetricGroupMeta[] = [
  {
    id: "price",
    label: "Price Overlays",
    description: "Rendered on the candlestick chart.",
  },
  {
    id: "volume",
    label: "Volume",
    description: "Volume throughput metrics.",
  },
  {
    id: "flow",
    label: "Flow",
    description: "Momentum in order flow and participation.",
  },
  {
    id: "risk",
    label: "Risk",
    description: "Volatility and range metrics.",
  },
  {
    id: "returns",
    label: "Returns",
    description: "Window-level return series.",
  },
];

export function metricLabel(metric: MetricName): string {
  return METRIC_META[metric].label;
}

export function metricDescription(metric: MetricName): string {
  return METRIC_META[metric].description;
}

export function metricColor(metric: MetricName): string {
  return METRIC_META[metric].color;
}

export function metricGroup(metric: MetricName): MetricGroupId {
  return METRIC_META[metric].group;
}

export function isOverlayMetric(metric: MetricName): boolean {
  return METRIC_META[metric].overlay;
}

// format different families of metrics
export function formatMetricValue(metric: MetricName, value: number | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";

  switch (METRIC_META[metric].format) {
    case "volume":
      return Intl.NumberFormat("en-US", {
        notation: Math.abs(value) >= 1000 ? "compact" : "standard",
        maximumFractionDigits: Math.abs(value) >= 1000 ? 2 : 4,
      }).format(value);
    case "percent":
      return `${(value * 100).toFixed(2)}%`;
    case "price":
      if (Math.abs(value) >= 1000) return value.toFixed(2);
      if (Math.abs(value) >= 1) return value.toFixed(4);
      return value.toFixed(6);
    case "number":
    default:
      return value.toFixed(6);
  }
}
