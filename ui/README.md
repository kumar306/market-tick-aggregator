# UI

The UI is a Next.js dashboard for inspecting the output of the full market data pipeline.

## Responsibilities

- query historical candles, metrics, and orderbook snapshots over REST
- subscribe to live tick and book updates over WebSocket
- render a candlestick chart with price overlays
- render grouped metric panels by family
- render a live orderbook pane with depth, best bid/ask, spread, and update time

## Dashboard Design

The dashboard is structured around one instrument view:

- top bar for exchange, symbol, and window selection
- sidebar for metric-family toggles
- main candlestick chart with price-like overlays
- grouped lower panels for volume, flow, risk, and returns
- orderbook pane on the bottom

This avoids plotting unrelated units on the same axis and makes the UI easier to review during demos.

## Data Flow

1. React Query fetches historical slices from the UI backend.
2. Zustand stores the merged chart-ready state.
3. REST data seeds historical series.
4. WebSocket updates incrementally extend or replace the latest points.
5. The chart layer reads from the store and keeps related panels time-synchronized.

This split is important to the UI architecture:

- React Query owns request lifecycle and caching
- Zustand owns the merged real-time view model
- chart components stay focused on rendering rather than stream reconciliation

## Key Areas

- `src/app/`: page composition
- `src/components/`: charts, sidebar, top bar, orderbook, layout
- `src/store/`: UI state and market stream state
- `src/services/`: REST and WebSocket clients
- `src/hooks/`: query and stream wiring
- `src/lib/metric-config.ts`: metric families, formatting, overlay rules

## Rendering Approach

The candlestick and metric panels are deliberately separated by semantic role:

- price-like metrics such as EMA, SMA, VWAP, and TWAP are rendered as overlays on the candle chart
- non-price metrics are grouped into lower panels by family
- all chart panels share time navigation so the dashboard behaves like a single instrument workspace rather than a set of unrelated charts

This keeps units interpretable and makes the dashboard easier to explain in demos.

## Testing and Validation

The UI is validated primarily through build, lint, and end-to-end behavior against the running stack.

Typical checks:

- `npm run lint`
- `npm run build`
- manual verification of REST seeding, WebSocket continuation, synchronized charts, and live orderbook updates

## Running

Development:

```bash
npm run dev
```

Build:

```bash
npm run build
```

Lint:

```bash
npm run lint
```
