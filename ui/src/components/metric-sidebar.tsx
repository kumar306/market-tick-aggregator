"use client";

import { metricDescription, metricGroup, metricLabel, METRIC_GROUPS } from "@/lib/metric-config";
import { useUIStore } from "@/store/ui-store";
import { ALL_METRICS } from "@/types/types";
import { useMemo } from "react";

const WINDOW_OPTIONS = ["5s", "10s", "30s", "1m", "2m", "5m", "10m", "30m", "1h"] as const;

export function MetricSidebar() {
  const metricSelection = useUIStore((s) => s.metricSelection);
  const toggleMetric = useUIStore((s) => s.toggleMetric);
  const setMetricEnabled = useUIStore((s) => s.setMetricEnabled);
  const depth = useUIStore((s) => s.depth);
  const setDepth = useUIStore((s) => s.setDepth);
  const windowId = useUIStore((s) => s.windowId);
  const setWindowId = useUIStore((s) => s.setWindowId);

  const selectedCount = useMemo(() => ALL_METRICS.filter((metric) => metricSelection[metric]).length, [metricSelection]);

  return (
    <aside className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold text-slate-800">Controls</h2>
          <p className="mt-1 text-xs text-slate-500">Select the active window, orderbook depth, and which observability metrics to render.</p>
        </div>
        <span className="rounded-full bg-slate-100 px-2 py-1 text-[11px] font-medium text-slate-600">
          {selectedCount}/{ALL_METRICS.length}
        </span>
      </div>

      <div className="mt-3">
        <label className="text-xs font-medium uppercase tracking-wide text-slate-500">Window</label>
        <select
          value={windowId}
          onChange={(e) => setWindowId(e.target.value)}
          className="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm outline-none transition focus:border-slate-500"
        >
          {WINDOW_OPTIONS.map((window) => (
            <option key={window} value={window}>
              {window}
            </option>
          ))}
        </select>
      </div>

      <div className="mt-3">
        <label className="text-xs font-medium uppercase tracking-wide text-slate-500">Orderbook Depth</label>
        <input
          type="number"
          min={1}
          max={50}
          value={depth}
          onChange={(e) => {
            const parsed = Number(e.target.value);
            if (!Number.isFinite(parsed)) return;
            setDepth(Math.max(1, Math.min(50, Math.floor(parsed))));
          }}
          className="mt-1 w-full rounded-lg border border-slate-300 px-2 py-1.5 text-sm outline-none transition focus:border-slate-500"
        />
      </div>

      <div className="mt-4">
        <p className="mb-2 text-xs font-medium uppercase tracking-wide text-slate-500">Metrics</p>

        <div className="mb-2 flex gap-2">
          <button
            type="button"
            onClick={() => {
              for (const metric of ALL_METRICS) setMetricEnabled(metric, true);
            }}
            className="rounded-md border border-slate-300 px-2 py-1 text-xs text-slate-600 transition hover:bg-slate-50"
          >
            Enable all
          </button>
          <button
            type="button"
            onClick={() => {
              for (const metric of ALL_METRICS) setMetricEnabled(metric, false);
            }}
            className="rounded-md border border-slate-300 px-2 py-1 text-xs text-slate-600 transition hover:bg-slate-50"
          >
            Clear
          </button>
        </div>

        <div className="max-h-[520px] space-y-3 overflow-auto pr-1">
          {METRIC_GROUPS.map((group) => {
            const metrics = ALL_METRICS.filter((metric) => metricGroup(metric) === group.id);
            return (
              <section key={group.id}>
                <div className="mb-1">
                  <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">{group.label}</p>
                  <p className="text-[11px] text-slate-500">{group.description}</p>
                </div>

                <div className="space-y-1">
                  {metrics.map((metric) => (
                    <label key={metric} className="flex cursor-pointer items-start justify-between gap-3 rounded-md border border-slate-200 px-2 py-2 text-sm transition hover:bg-slate-50">
                      <div>
                        <div className="text-slate-700">{metricLabel(metric)}</div>
                        <div className="text-[11px] text-slate-500">{metricDescription(metric)}</div>
                      </div>
                      <input
                        type="checkbox"
                        checked={metricSelection[metric]}
                        onChange={() => toggleMetric(metric)}
                        className="mt-0.5 h-4 w-4 cursor-pointer accent-slate-700"
                      />
                    </label>
                  ))}
                </div>
              </section>
            );
          })}
        </div>
      </div>
    </aside>
  );
}
