import type {
    CandleDTO,
    MetricResultDTO,
    MetricName,
    OrderbookDTO
} from "@/types/types"

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8080"

type DateInput = Date | string;
function ToIso(value: DateInput): string {
    return value instanceof Date ? value.toISOString() : value;
}

export interface CandleParams {
    exchange: string;
    symbol: string;
    from: DateInput;
    to: DateInput;
}

export interface MetricParams {
    exchange: string;
    symbol: string;
    from: DateInput;
    to: DateInput;
    windows: string[];
    metrics: MetricName[];
}

export interface OrderbookParams {
    exchange: string;
    symbol: string;
    depth: number;
}

async function getJson<T>(path: string, params: URLSearchParams): Promise<T> {
    const query = params.toString()
    const url = `${API_BASE_URL}${path}${query ? `?${query}`: ""}`

    const res = await fetch(url, {
        method: "GET",
        headers: { Accept: "application/json" },
        cache: "no-store"
    })

    if(!res.ok) {
        const body = await res.text().catch(() => "");
        throw new Error(`GET ${path} failed: ${res.status} ${body}`)
    }

    return (await res.json()) as T;
}

export async function getCandles(params: CandleParams): Promise<CandleDTO[]> {
    const queryParam = new URLSearchParams({
        exchange: params.exchange,
        symbol: params.symbol,
        from: ToIso(params.from),
        to: ToIso(params.to)
    })

    return getJson<CandleDTO[]>("/api/candles", queryParam)
}

export async function getMetrics(params: MetricParams): Promise<MetricResultDTO> {
    const queryParam = new URLSearchParams({
        exchange: params.exchange,
        symbol: params.symbol,
        from: ToIso(params.from),
        to: ToIso(params.to)
    })

    for (const window of params.windows) queryParam.append("windows", window)
    for (const metric of params.metrics) queryParam.append("metrics", metric)

    return getJson<MetricResultDTO>("/api/metrics", queryParam)
}

export async function getOrderbook(params: OrderbookParams): Promise<OrderbookDTO> {
    const queryParam = new URLSearchParams({
        exchange: params.exchange,
        symbol: params.symbol,
        depth: params.depth.toString()
    })

    return getJson<OrderbookDTO>("/api/orderbook", queryParam)
}