import type {
    OrderbookStreamMessage,
    TickStreamMessage,
    WSSubscriptionType
} from "@/types/types"

const WS_URL = process.env.NEXT_PUBLIC_WS_URL ?? "ws://localhost:8080/ws";

export interface WSSubscribeMessage {
    type: WSSubscriptionType;
    exchange: string;
    symbol: string;
}

type ConnStatus = "idle" | "connecting" | "open" | "closed"

// let ws not know any other information other than receiving messages and emitting them as events
// events handled in zustand/react query cache - and observers will re-render
type Handlers = {
    onTick?: (msg: TickStreamMessage) => void;
    onBook?: (msg: OrderbookStreamMessage) => void;
    onStatusChange?: (status: ConnStatus) => void;
    onError?: (err: unknown) => void;
}

// cast the input message into tick message or book messages
function isTickMessage(data: unknown): data is TickStreamMessage {
    return !!data && typeof data === "object" && "window_id" in data;
}

function isBookMessage(data: unknown): data is OrderbookStreamMessage {
    return !!data && typeof data === "object" && "bids" in data && "asks" in data;
}

export class WSClient {
    private ws: WebSocket | null = null;
    private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    private status: ConnStatus = "idle";

    // let handlers not be able to be mutated later on. passed into ws client which handles its emitted events
    private readonly handlers: Handlers;

    // let client be able to hold only 1 active connection at a time for tick/book
    // if client changes symbols/exchanges, this subscriptions overwritten and existing open connection is closed
    // so the server stops sending feed for old subscribed key and now sends for only new key
    private subscriptions = new Map<WSSubscriptionType, WSSubscribeMessage>();

    constructor(handlers: Handlers = {}) {
        this.handlers = handlers
    }

    getStatus(): ConnStatus {
        return this.status;
    }

    // emit event upon status change
    private setStatus(next: ConnStatus): void {
        this.status = next;
        this.handlers.onStatusChange?.(next);
    }

    setSubscription(next: WSSubscribeMessage): void {
        const prev = this.subscriptions.get(next.type)
        const changed = !prev || prev.exchange !== next.exchange || prev.symbol !== next.symbol;

        this.subscriptions.set(next.type, next);

        if(!changed) return;

        // if already connected and changed, reconnect to clear backend registration
        if(prev && this.ws?.readyState === WebSocket.OPEN) {
            this.reconnect();
            return;
        }

        // if connection is open but not consuming yet, then send the subscribe message
        if(this.ws?.readyState === WebSocket.OPEN) {
            this.sendSubscribe(next);
        }
    }

    connect(): void {
        // prevent exec during ssr
        if(typeof window === "undefined") return;

        if(this.ws && (this.ws.readyState == WebSocket.OPEN || this.ws.readyState == WebSocket.CONNECTING)) {
            return;
        }

        this.setStatus("connecting")
        this.ws = new WebSocket(WS_URL)

        this.ws.onopen = () => {
            this.setStatus("open")
            for (const sub of this.subscriptions.values()) this.sendSubscribe(sub)
        };

        // emit the events onmessage
        this.ws.onmessage = (event) => {
            try {
                const parsed = JSON.parse(event.data as string) as unknown;
                if(isTickMessage(parsed)) this.handlers.onTick?.(parsed);
                else if (isBookMessage(parsed)) this.handlers.onBook?.(parsed);
            } catch (err) {
                this.handlers.onError?.(err);
            }
        }

        this.ws.onerror = (event) => {
            this.handlers.onError?.(event);
        }

        this.ws.onclose = () => {
            this.setStatus("closed")
            // in case the connection got closed unexpectedly
            this.scheduleReconnect();
        }
    }

    // send the subcription message for tick and book to the backend's readPump which registers it to list of subscribed clients
    private sendSubscribe(sub: WSSubscribeMessage): void {
        if(!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
        this.ws.send(JSON.stringify(sub))
    }

    private scheduleReconnect(): void {
        // if reconnect already scheduled, then no-op. else schedule connect after 2 seconds
        if(this.reconnectTimer) return;

        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            this.connect()
        }, 2000);
    }

    // reset the connection to receive for new feed and stop receiving old feed messages
    private reconnect(): void {
        if(this.ws && (this.ws.readyState == WebSocket.OPEN || this.ws.readyState == WebSocket.CONNECTING)) {
            close();
        }
        this.connect();
    }

    // cleanup
    close(): void {
        if(this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }

        const socket = this.ws;
        this.ws = null;
        this.setStatus("closed")

        if(socket && (socket.readyState == WebSocket.OPEN || socket.readyState == WebSocket.CONNECTING)) {
            socket.close();
        }
    }
}