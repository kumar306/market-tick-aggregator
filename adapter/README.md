todo: refactor main.go

Coinbase orderbook is wired through `level2_batch` in the adapter so the feed can be consumed
without websocket auth, while the downstream pipeline still treats it as logical `level2`.

20/02/2026:
1. missing binance OHLC data in ticks table. so need to subscribe to its kline stream later
