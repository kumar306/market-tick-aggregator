import { useEffect, useRef } from "react";
import { WSClient } from "@/services/ws";
import { useUIStore } from "@/store/ui-store";
import { useMarketStore } from "@/store/market-store";

export function useMarketStream() {
    const exchange = useUIStore((s) => s.exchange);
    const symbol = useUIStore((s) => s.symbol);

    const setWsStatus = useMarketStore((s) => s.setWsStatus);
    const upsertTick = useMarketStore((s) => s.upsertTick);
    const upsertBook = useMarketStore((s) => s.upsertBook);

    // to have a persistent websocket connection across rerenders
    const clientRef = useRef<WSClient | null>(null);

    useEffect(() => {
        const client = new WSClient({
            onTick: upsertTick,
            onBook: upsertBook,
            onStatusChange: setWsStatus,
            onError: (err) => {
                console.error("ws error", err);
            },
        });

        clientRef.current = client;
        client.connect();

        return () => {
            client.close();
            clientRef.current = null;
        }
    }, [upsertTick, upsertBook, setWsStatus]);


    useEffect(() => {
        if(!exchange || !symbol || !clientRef.current) return;

        clientRef.current.setSubscription({ type: "tick", exchange, symbol });
        clientRef.current.setSubscription({ type: "book", exchange, symbol });
    }, [exchange, symbol])
}