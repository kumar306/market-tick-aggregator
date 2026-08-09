// Mock exchange WebSocket server for E2E

// exactly mocks binance, coinbase, kraken subscribe, ping handle, emit data messages
// adapter processes these process as it would real exchange traffic, at a
// controlled synthetic rate. One binary, exchange selected via EXCHANGE env when running the full load test.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var currentRate atomic.Int64

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	exchange := envOr("EXCHANGE", "binance")
	port := envOr("PORT", "8081")
	initialRate, _ := strconv.Atoi(envOr("RATE_PER_SEC", "50"))
	if initialRate < 1 {
		initialRate = 1
	}
	currentRate.Store(int64(initialRate))

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("upgrade error: %v", err)
			return
		}
		defer conn.Close()
		handleConnection(exchange, conn)
	})

	http.HandleFunc("/rate", func(w http.ResponseWriter, r *http.Request) {
		n, err := strconv.Atoi(r.URL.Query().Get("value"))
		if err != nil || n < 1 {
			http.Error(w, "invalid rate value", http.StatusBadRequest)
			return
		}
		currentRate.Store(int64(n))
		fmt.Fprintf(w, "rate set to %d/s\n", n)
	})

	// mimics binance depth-snapshot endpoint - adapter's resync path
	// calls this once per symbol on cold start, independent of the WS feed,
	// so it needs its own mock regardless of which exchange this pod is
	http.HandleFunc("/api/v3/depth", func(w http.ResponseWriter, r *http.Request) {
		price := 65_000.0 + rand.Float64()*200 - 100
		resp := map[string]any{
			"lastUpdateId": time.Now().UnixMilli(),
			"bids": [][]string{
				{fmt.Sprintf("%.2f", price), "1.5"},
				{fmt.Sprintf("%.2f", price-1), "2.0"},
			},
			"asks": [][]string{
				{fmt.Sprintf("%.2f", price+1), "1.2"},
				{fmt.Sprintf("%.2f", price+2), "0.8"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	log.Printf("Mock %s exchange server listening on :%s/ws (initial rate=%d/s)", exchange, port, initialRate)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleConnection(exchange string, conn *websocket.Conn) {
	// adapter's Subscribe() writes one request then blocks on one ReadMessage
	// for the ack -- mirror that exactly, single write/read, before streaming.
	_, subMsg, err := conn.ReadMessage()
	if err != nil {
		log.Printf("read subscribe error: %v", err)
		return
	}

	channel, symbols, err := handleSubscribe(exchange, conn, subMsg)
	if err != nil {
		log.Printf("subscribe ack error: %v", err)
		return
	}

	// Drain further reads in the background so gorilla's default ping
	// handler can auto-respond with pongs to adapter's heartbeat pings.
	readErrCh := make(chan struct{})
	go func() {
		defer close(readErrCh)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	var seq int64
	for {
		select {
		case <-readErrCh:
			return
		default:
		}

		seq++
		msg := buildDataMessage(exchange, channel, symbols, seq)
		if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}

		// re-read the rate on every message so a live rate change takes
		// effect within one message interval, instead of needing a redeploy.
		time.Sleep(time.Second / time.Duration(currentRate.Load()))
	}
}

func handleSubscribe(exchange string, conn *websocket.Conn, subMsg []byte) (channel string, symbols []string, err error) {
	switch exchange {
	case "binance":
		var req struct {
			Method string   `json:"method"`
			Params []string `json:"params"`
			Id     int      `json:"id"`
		}
		if err = json.Unmarshal(subMsg, &req); err != nil {
			return
		}
		for _, p := range req.Params {
			parts := strings.SplitN(p, "@", 2)
			if len(parts) == 2 {
				symbols = append(symbols, strings.ToUpper(parts[0]))
				channel = parts[1]
			}
		}
		ack := map[string]any{"result": nil, "id": req.Id}
		b, _ := json.Marshal(ack)
		err = conn.WriteMessage(websocket.TextMessage, b)

	case "coinbase":
		var req struct {
			Type       string   `json:"type"`
			ProductIds []string `json:"product_ids"`
			Channels   []string `json:"channels"`
		}
		if err = json.Unmarshal(subMsg, &req); err != nil {
			return
		}
		symbols = req.ProductIds
		if len(req.Channels) > 0 {
			channel = req.Channels[0]
		}
		ack := map[string]any{
			"type": "subscriptions",
			"channels": []map[string]any{
				{"name": channel, "product_ids": symbols},
			},
		}
		b, _ := json.Marshal(ack)
		err = conn.WriteMessage(websocket.TextMessage, b)

	case "kraken":
		var req struct {
			Method string `json:"method"`
			Params struct {
				Channel string   `json:"channel"`
				Symbol  []string `json:"symbol"`
			} `json:"params"`
		}
		if err = json.Unmarshal(subMsg, &req); err != nil {
			return
		}
		channel = req.Params.Channel
		symbols = req.Params.Symbol
		ack := map[string]any{"method": "subscribe", "success": true}
		b, _ := json.Marshal(ack)
		err = conn.WriteMessage(websocket.TextMessage, b)

	default:
		err = fmt.Errorf("unknown exchange %q", exchange)
	}
	return
}

func pickSymbol(symbols []string, seq int64) string {
	if len(symbols) == 0 {
		return "BTCUSDT"
	}
	return symbols[seq%int64(len(symbols))]
}

// probability a book-side update is a cancel (qty=0, i.e. "remove this price
// level") rather than a resting order. Real depth feeds send these
// constantly; without them a consumer's in-memory book only ever grows for
// the life of the process, which is a real bug this mock was masking.
const cancelProbability = 0.15

func bookLevelQty() float64 {
	if rand.Float64() < cancelProbability {
		return 0
	}
	return 0.1 + rand.Float64()
}

func buildDataMessage(exchange, channel string, symbols []string, seq int64) []byte {
	sym := pickSymbol(symbols, seq)
	nowMs := time.Now().UnixMilli()
	price := 65_000.0 + rand.Float64()*200 - 100
	qty := 0.1 + rand.Float64()

	var msg map[string]any

	switch exchange {
	case "binance":
		if channel == "depth" {
			bidQty, askQty := bookLevelQty(), bookLevelQty()
			msg = map[string]any{
				"e": "depthUpdate", "E": nowMs, "T": nowMs, "s": sym,
				"U": seq, "u": seq + 1, "pu": seq - 1,
				"b": [][]string{{fmt.Sprintf("%.2f", price), fmt.Sprintf("%.4f", bidQty)}},
				"a": [][]string{{fmt.Sprintf("%.2f", price+1), fmt.Sprintf("%.4f", askQty)}},
			}
		} else {
			msg = map[string]any{
				"e": "aggTrade", "E": nowMs, "s": sym, "a": seq,
				"p": fmt.Sprintf("%.2f", price), "q": fmt.Sprintf("%.4f", qty),
				"f": seq, "l": seq, "T": nowMs, "m": seq%2 == 0,
			}
		}

	case "coinbase":
		if channel == "level2_batch" || channel == "level2" {
			msg = map[string]any{
				"type": "l2update", "product_id": sym,
				"changes": [][]string{{"buy", fmt.Sprintf("%.2f", price), fmt.Sprintf("%.4f", bookLevelQty())}},
				"time":    time.Now().UTC().Format(time.RFC3339Nano),
			}
		} else {
			msg = map[string]any{
				"type": "ticker", "sequence": seq, "product_id": sym,
				"price": fmt.Sprintf("%.2f", price), "open_24h": fmt.Sprintf("%.2f", price-50),
				"volume_24h": "1000.0", "low_24h": fmt.Sprintf("%.2f", price-100),
				"high_24h": fmt.Sprintf("%.2f", price+100), "best_bid": fmt.Sprintf("%.2f", price-1),
				"best_ask": fmt.Sprintf("%.2f", price+1), "side": "buy",
				"time": time.Now().UTC().Format(time.RFC3339Nano), "trade_id": seq,
				"last_size": fmt.Sprintf("%.4f", qty),
			}
		}

	case "kraken":
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if channel == "book" {
			msg = map[string]any{
				"channel": "book", "type": "update",
				"data": []map[string]any{
					{
						"symbol":    sym,
						"bids":      []map[string]float64{{"price": price, "qty": bookLevelQty()}},
						"asks":      []map[string]float64{{"price": price + 1, "qty": bookLevelQty()}},
						"checksum":  seq,
						"timestamp": now,
					},
				},
			}
		} else {
			msg = map[string]any{
				"channel": "ticker", "type": "update",
				"data": []map[string]any{
					{
						"symbol": sym, "bid": price - 1, "bid_qty": qty,
						"ask": price + 1, "ask_qty": qty, "last": price,
						"volume": qty * 1000, "vwap": price, "low": price - 100, "high": price + 100,
						"change": 0.0, "change_pct": 0.0, "timestamp": now,
					},
				},
			}
		}
	}

	b, _ := json.Marshal(msg)
	return b
}
