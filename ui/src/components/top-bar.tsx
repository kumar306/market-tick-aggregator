"use client";

import { WSStatus } from "@/store/market-store";
import { useUIStore } from "@/store/ui-store";
import { useMemo } from "react";

function dotClass(status: WSStatus): string {
    if(status === "open") return "bg-emerald-500";
    if(status === "connecting") return "bg-amber-500";
    return "bg-rose-500";
}

const PAIRS = [
  { exchange: "binance", symbol: "BTCUSDT" },
  { exchange: "coinbase", symbol: "BTC-USD" },
  { exchange: "kraken", symbol: "BTC/USD" },
] as const;

export function TopBar({ wsState }: { wsState: WSStatus }) {
    const exchange = useUIStore((s) => s.exchange);
    const symbol = useUIStore((s) => s.symbol);
    const setPair = useUIStore((s) => s.setPair);

    const exchanges = useMemo(() => PAIRS.map((pair) => pair.exchange), []);
    const symbolsForExchange = useMemo(() => {
      return PAIRS.filter((pair) => pair.exchange === exchange).map((pair) => pair.symbol);
    }, [exchange]);

     return (
    <header className="flex flex-wrap items-center gap-3 rounded-xl border bg-white p-4">
      <h1 className="mr-2 text-lg font-semibold">Market UI</h1>

      <label className="text-sm text-slate-600">
        Exchange
        <select
          className="ml-2 rounded-md border px-2 py-1 text-sm"
          value={exchange}
          onChange={(e) => {
            const next = PAIRS.find((pair) => pair.exchange === e.target.value);
            if (!next) return;
            setPair(next.exchange, next.symbol);
          }}
        >
          {exchanges.map((x) => (
            <option key={x} value={x}>
              {x}
            </option>
          ))}
        </select>
      </label>

      <label className="text-sm text-slate-600">
        Symbol
        <select
          className="ml-2 rounded-md border px-2 py-1 text-sm"
          value={symbol}
          onChange={(e) => setPair(exchange, e.target.value)}
        >
          {symbolsForExchange.map((x) => (
            <option key={x} value={x}>
              {x}
            </option>
          ))}
        </select>
      </label>

      <div className="ml-auto flex items-center gap-2 text-sm">
        <span className={`h-2.5 w-2.5 rounded-full ${dotClass(wsState)}`} />
        <span className="capitalize text-slate-600">ws: {wsState}</span>
      </div>
    </header>
  );
}
