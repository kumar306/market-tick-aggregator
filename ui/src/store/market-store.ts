import { create } from "zustand";
import { OrderbookStreamMessage, TickStreamMessage } from "@/types/types"

export type WSStatus = "idle" | "connecting" | "open" | "closed"

interface MarketStreamState {
    wsState: WSStatus;
    ticksByKey: Record<string, TickStreamMessage>;
    booksByKey: Record<string, OrderbookStreamMessage>;
    setWsStatus: (status: WSStatus) => void;
    upsertTick: (tick: TickStreamMessage) => void;
    upsertBook: (book: OrderbookStreamMessage) => void;
    clearKey: (key: string) => void;
}

export const useMarketStore = create<MarketStreamState>((set) => ({
    wsState: "idle",
    ticksByKey: {},
    booksByKey: {},
    setWsStatus: (status) => set({ wsState: status }),
    upsertTick: (tick) => set((state) => {
        const key = `${tick.exchange}:${tick.symbol}`;
        const prev = state.ticksByKey[key];
        if(prev && prev.end_ts_ms == tick.end_ts_ms && prev.start_ts_ms == tick.start_ts_ms) {
            return state;
        }
        return {
            ticksByKey: {
                ...state.ticksByKey,
                [key]: tick,
            }
        }
    }),
    upsertBook: (book) => set((state) => {
        const key = `${book.exchange}:${book.symbol}`
        const prev = state.booksByKey[key]
        if(prev && prev.event_time == book.event_time) {
            return state;
        }
        return {
            booksByKey: {
                ...state.booksByKey,
                [key]: book
            }
        }
    }),
    clearKey: (key) => set((state) => {
        const nextTicks = { ...state.ticksByKey }
        const nextBooks = { ...state.booksByKey }
        delete nextTicks[key];
        delete nextBooks[key];
        return { ticksByKey: nextTicks, booksByKey: nextBooks };
    })
}));

export const selectTickByKey = (key:string) => (state: MarketStreamState): TickStreamMessage | undefined => state.ticksByKey[key];
export const selectBookByKey = (key: string) => (state: MarketStreamState): OrderbookStreamMessage | undefined => state.booksByKey[key];