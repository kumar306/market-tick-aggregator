"use client";

import { memo, useMemo } from "react";
import { selectBookByKey, useMarketStore } from "@/store/market-store";
import { OrderbookDTO, OrderbookLevelDTO, OrderbookStreamMessage } from "@/types/types";

interface OrderbookPanelProps {
  exchange: string;
  symbol: string;
  depth: number;
  restBook?: OrderbookDTO;
  loading: boolean;
}

interface Level {
  price: number;
  volume: number;
}

interface DisplayBook {
  bids: Level[];
  asks: Level[];
  bestBid?: Level;
  bestAsk?: Level;
  spread?: number;
  crossed: boolean;
  updatedLabel: string;
  source: "live" | "snapshot" | "empty";
}

function toUpdatedLabel(book: OrderbookStreamMessage): string {
  if (Number.isFinite(book.event_time_millis)) {
    return new Date(book.event_time_millis as number).toLocaleTimeString();
  }
  if (book.event_time) {
    const parsed = Date.parse(book.event_time);
    if (Number.isFinite(parsed)) {
      return new Date(parsed).toLocaleTimeString();
    }
  }
  return "n/a";
}

function pickLevelsBySide(levels: Record<string, OrderbookLevelDTO[]>, side: "bids" | "asks"): Level[] {
  const sideKeys =
    side === "bids"
      ? ["bids", "bid", "b", "buy"]
      : ["asks", "ask", "s", "sell"];

  for (const key of sideKeys) {
    const rows = levels[key];
    if (!rows || rows.length === 0) continue;
    return [...rows]
      .sort((a, b) => a.level_index - b.level_index)
      .map((row) => ({ price: row.price, volume: row.volume }));
  }

  return [];
}

function toDisplayBook(restBook: OrderbookDTO | undefined, liveBook: OrderbookStreamMessage | undefined, depth: number): DisplayBook {
  if (liveBook) {
    const bids = [...(liveBook.bids ?? [])]
      .sort((a, b) => b.price - a.price)
      .slice(0, depth);
    const asks = [...(liveBook.asks ?? [])]
      .sort((a, b) => a.price - b.price)
      .slice(0, depth);

    const bestBid = liveBook.bestBid ?? bids[0];
    const bestAsk = liveBook.bestAsk ?? asks[0];
    const spread = Number.isFinite(liveBook.spread)
      ? liveBook.spread
      : bestBid && bestAsk
        ? bestAsk.price - bestBid.price
        : undefined;

    return {
      bids,
      asks,
      bestBid,
      bestAsk,
      spread,
      crossed: typeof spread === "number" && spread < 0,
      source: "live",
      updatedLabel: toUpdatedLabel(liveBook),
    };
  }

  if (restBook) {
    const bids = pickLevelsBySide(restBook.levels, "bids").slice(0, depth);
    const asks = pickLevelsBySide(restBook.levels, "asks").slice(0, depth);

    return {
      bids,
      asks,
      bestBid: Number.isFinite(restBook.best_bid_price)
        ? { price: restBook.best_bid_price, volume: restBook.best_bid_volume }
        : bids[0],
      bestAsk: Number.isFinite(restBook.best_ask_price)
        ? { price: restBook.best_ask_price, volume: restBook.best_ask_volume }
        : asks[0],
      spread: Number.isFinite(restBook.spread) ? restBook.spread : undefined,
      crossed: Number.isFinite(restBook.spread) ? restBook.spread < 0 : false,
      source: "snapshot",
      updatedLabel: new Date(restBook.event_time).toLocaleTimeString(),
    };
  }

  return {
    bids: [],
    asks: [],
    crossed: false,
    source: "empty",
    updatedLabel: "n/a",
  };
}

function formatPrice(value: number | undefined): string {
  if (!Number.isFinite(value)) return "-";
  return (value as number).toFixed(2);
}

function formatSpread(value: number | undefined, crossed: boolean): string {
  if (!Number.isFinite(value)) return "-";
  const spread = value as number;
  return crossed ? `Crossed ${Math.abs(spread).toFixed(2)}` : spread.toFixed(2);
}

function formatVolume(value: number | undefined): string {
  if (!Number.isFinite(value)) return "-";
  const n = value as number;
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return n.toFixed(4);
}

function sideVolumeMax(levels: Level[]): number {
  const max = levels.reduce((acc, level) => Math.max(acc, level.volume), 0);
  return max > 0 ? max : 1;
}

function BookSide({
  title,
  tone,
  levels,
}: {
  title: string;
  tone: "bid" | "ask";
  levels: Level[];
}) {
  const maxVol = sideVolumeMax(levels);
  const barClass = tone === "bid" ? "bg-emerald-100" : "bg-rose-100";
  const priceClass = tone === "bid" ? "text-emerald-700" : "text-rose-700";

  return (
    <section className="rounded-xl border border-slate-200 bg-slate-50/60 p-3">
      <div className="mb-2 text-xs font-semibold tracking-wide text-slate-600">{title}</div>
      <div className="space-y-1">
        {levels.length > 0 ? (
          levels.map((level, idx) => {
            const widthPercent = Math.min(100, (level.volume / maxVol) * 100);
            return (
              <div key={`${title}-${idx}-${level.price}`} className="relative overflow-hidden rounded-md border border-slate-200 bg-white px-2 py-1.5 text-xs">
                <div className={`absolute inset-y-0 left-0 ${barClass}`} style={{ width: `${widthPercent}%` }} />
                <div className="relative z-10 grid grid-cols-2 gap-2">
                  <span className={`${priceClass} font-medium`}>{formatPrice(level.price)}</span>
                  <span className="text-right text-slate-700">{formatVolume(level.volume)}</span>
                </div>
              </div>
            );
          })
        ) : (
          <div className="rounded-md border border-dashed border-slate-300 bg-white px-2 py-5 text-center text-xs text-slate-500">No levels</div>
        )}
      </div>
    </section>
  );
}

export const OrderbookPanel = memo(function OrderbookPanel({
  exchange,
  symbol,
  depth,
  restBook,
  loading,
}: OrderbookPanelProps) {
  const key = `${exchange}:${symbol}`;
  const selector = useMemo(() => selectBookByKey(key), [key]);
  const liveBook = useMarketStore(selector);

  const book = useMemo(() => toDisplayBook(restBook, liveBook, depth), [restBook, liveBook, depth]);
  const isEmpty = book.source === "empty";

  return (
    <article className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-sm font-semibold text-slate-800">Orderbook ({exchange}:{symbol})</h2>
        <div className="flex items-center gap-2 text-xs text-slate-500">
          <span>{loading && !liveBook ? "loading..." : `depth: ${depth}`}</span>
          <span className={`rounded-full px-2 py-0.5 ${book.source === "live" ? "bg-emerald-100 text-emerald-700" : "bg-slate-100 text-slate-600"}`}>
            {book.source === "live" ? "live" : "snapshot"}
          </span>
        </div>
      </div>

      <div className="mb-3 grid gap-2 rounded-xl border border-slate-200 bg-slate-50/70 p-3 text-xs text-slate-700 sm:grid-cols-4">
        <div>
          <div className="text-slate-500">Best Bid</div>
          <div className="font-semibold text-emerald-700">{formatPrice(book.bestBid?.price)}</div>
        </div>
        <div>
          <div className="text-slate-500">Best Ask</div>
          <div className="font-semibold text-rose-700">{formatPrice(book.bestAsk?.price)}</div>
        </div>
        <div>
          <div className="text-slate-500">Spread</div>
          <div className={`font-semibold ${book.crossed ? "text-rose-700" : ""}`}>{formatSpread(book.spread, book.crossed)}</div>
        </div>
        <div>
          <div className="text-slate-500">Updated</div>
          <div className="font-semibold">{book.updatedLabel}</div>
        </div>
      </div>

      {book.crossed ? (
        <div className="mb-3 rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
          The latest book snapshot is crossed: best ask is below best bid. That usually indicates stale or inconsistent upstream orderbook state rather than a UI issue.
        </div>
      ) : null}

      {!isEmpty ? (
        <div className="grid gap-3 lg:grid-cols-2">
          <BookSide title="Bids" tone="bid" levels={book.bids} />
          <BookSide title="Asks" tone="ask" levels={book.asks} />
        </div>
      ) : (
        <div className="rounded-xl border border-dashed border-slate-300 px-3 py-8 text-center text-sm text-slate-500">
          Waiting for orderbook snapshot or websocket updates.
        </div>
      )}
    </article>
  );
});
