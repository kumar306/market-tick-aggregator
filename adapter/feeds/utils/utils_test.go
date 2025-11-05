package utils_test

import (
	"encoding/json"
	"market-adapter/feeds/binance"
	"market-adapter/feeds/coinbase"
	"market-adapter/feeds/kraken"
	"market-adapter/feeds/utils"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// i will need to spin up a mock websocket server to read messages and write mock ack response
// for unit testing subscribe and ping functionality
func newMockServer(t *testing.T, handler func(*websocket.Conn)) *httptest.Server {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("Error spinning up mock websocket server with error: %v", err)
		}

		go handler(conn)

	}))

	return server
}

func convertToWsUrl(url string) string {
	if strings.HasPrefix(url, "http://") {
		return "ws://" + strings.TrimPrefix(url, "http://")
	} else if strings.HasPrefix(url, "https://") {
		return "ws://" + strings.TrimPrefix(url, "https://")
	}
	return url
}

// table driven test to send the dynamic subscribe req and server sends dynamic dummy ack response
func Test_SendAndAckSubscribe(t *testing.T) {

	// binance
	binanceSubscribeMsg := binance.BinanceSubscribeMessage{
		Method: "SUBSCRIBE",
		Params: []string{"btcusdt@aggTrade"},
		Id:     1}

	var binanceResponse struct {
		Result interface{} `json:"result"`
		Id     int         `json:"id"`
		Code   int         `json:"code"`
		Msg    string      `json:"msg"`
	}

	// coinbase
	coinbaseSubscribeMsg := coinbase.CBSubscribeRequest{
		Type:       "subscribe",
		ProductIds: []string{"ETH-USD"},
		Channels:   []string{"ticker"},
	}

	var coinbaseResponse coinbase.CBSubscribeResponse

	// kraken
	krakenSubscribeMsg := kraken.KrakenSubscribeRequest{
		Method: "subscribe",
		Params: kraken.KrakenSubscribeParam{
			Channel: "ticker",
			Symbol:  []string{"BTC/USD"},
		},
	}

	var krakenResponse kraken.KrakenSubscribeResponse

	// req, res, mock server response, feedname, channel - extend this for other exchanges
	tests := []struct {
		req           interface{}
		res           interface{}
		mockServerRes string
		feed          string
		channel       string
	}{
		{binanceSubscribeMsg, binanceResponse, `{"result":null, "id": 1}`, "Binance", "aggTrade"},
		{coinbaseSubscribeMsg, coinbaseResponse, `{
			"type":"subscriptions", 
			"channels":[
				{
					"name":"ticker",
					"product_ids":["ETH-USD"]
				}
			]
		}`, "Coinbase", "ticker"},
		{krakenSubscribeMsg, krakenResponse, `{
			"method": "subscribe",
			"result": {
				"channel": "ticker",
				"snapshot": true,
				"symbol": "ALGO/USD"
			},
			"success": true,
			"time_in": "2023-09-25T09:04:31.742599Z",
			"time_out": "2023-09-25T09:04:31.742648Z"
		}`, "Kraken", "ticker"},
	}

	for _, tc := range tests {
		server := newMockServer(t, func(conn *websocket.Conn) {
			defer conn.Close()
			for {
				_, msg, err := conn.ReadMessage()
				if err != nil {
					return
				}
				t.Logf("Server received subscribe message: %v", string(msg))
				// subscribe dummy response
				resp := tc.mockServerRes
				conn.WriteMessage(websocket.TextMessage, []byte(resp))

			}
		})
		defer server.Close()

		conn, _, err := websocket.DefaultDialer.Dial(convertToWsUrl(server.URL), nil)
		if err != nil {
			t.Fatalf("Unable to connect to mock server. Error: %v", err)
		}
		defer conn.Close()

		err = utils.SendAndAckSubscribe(conn, tc.req, &tc.res, tc.feed, tc.channel)
		if err != nil {
			t.Fatalf("Error in subscription test: %v", err)
		}
	}

}

func Test_SendPing(t *testing.T) {
	server := newMockServer(t, func(conn *websocket.Conn) {
		t.Logf("Entering ping test..")
		defer conn.Close()
		// this guy runs only if some ping frames are even being read from connection
		// which is why i had to keep reading using conn.ReadMessage
		conn.SetPingHandler(func(appData string) error {
			t.Logf("Server received ping frame. Sending pong frame..")
			time.Sleep(20 * time.Millisecond)
			conn.WriteMessage(websocket.PongMessage, nil)
			return nil
		})

		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				t.Logf("Server connection closed or read error: %v", err)
				return
			}
		}
	})
	defer server.Close()

	conn, _, err := websocket.DefaultDialer.Dial(convertToWsUrl(server.URL), nil)
	if err != nil {
		t.Fatalf("Error connecting to mock server: %v", err)
	}
	defer conn.Close()

	pingErr := utils.SendPing(conn, &sync.Mutex{}, "Binance")
	if pingErr != nil {
		t.Fatalf("Error in ping util: %v", pingErr)
	}
}

/*
	table driven test for normalizers - all exchanges

i. binance ticker
ii. binance depth
iii. coinbase ticker
iv. coinbase L2 snapshot
v. coinbase L2 update
vi. kraken ticker
vii. kraken book snapshot
viii. kraken book update
*/
func Test_NormalizeFeeds(t *testing.T) {
	tests := []struct {
		raw       string
		symbolKey string
		feed      string
		channel   string
		symbol    string
	}{
		{`{"s":"BTCUSDT","p":"0.001","q":"100"}`, "s", "Binance", "aggTrade", "BTCUSDT"},
		{`{"s":"BTCUSDT","pu":149,"b": [["0.0024","10"]],"a": [["0.0026","100"]]}`, "s", "Binance", "depth", "BTCUSDT"},
		{`{"type": "ticker","sequence": 37475248783,"product_id": "ETH-USD","price": "1285.22","open_24h": "1310.79","volume_24h": "245532.79269678"}`, "product_id", "Coinbase", "ticker", "ETH-USD"},
		{`{"type": "snapshot","product_id": "BTC-USD","bids": [["10101.10", "0.45054140"]],"asks": [["10102.55", "0.57753524"]]}`, "product_id", "Coinbase", "level2", "BTC-USD"},
		{`{"type": "l2update","product_id": "BTC-USD","changes": [["buy","22356.270000","0.00000000"],["buy","22356.300000","1.00000000"]],"time": "2022-08-04T15:25:05.010758Z"}`, "product_id", "Coinbase", "level2", "BTC-USD"},
		{`{"channel": "ticker","type": "snapshot","data": [
        {
            "symbol": "ALGO/USD",
            "bid": 0.10025,
            "bid_qty": 740.0,
            "ask": 0.10036,
            "ask_qty": 1361.44813783,
            "last": 0.10035,
            "volume": 997038.98383185,
            "vwap": 0.10148,
            "low": 0.09979,
            "high": 0.10285,
            "change": -0.00017,
            "change_pct": -0.17
        }
    ]
}`, "symbol", "Kraken", "ticker", "ALGO/USD"},
		{`{
    "channel": "book",
    "type": "snapshot",
    "data": [
        {
            "symbol": "MATIC/USD",
            "bids": [
                {
                    "price": 0.5666,
                    "qty": 4831.75496356
                },
                {
                    "price": 0.5665,
                    "qty": 6658.22734739
                },
                {
                    "price": 0.5664,
                    "qty": 18724.91513344
                },
                {
                    "price": 0.5663,
                    "qty": 11563.92544914
                },
                {
                    "price": 0.5662,
                    "qty": 14006.65365711
                },
                {
                    "price": 0.5661,
                    "qty": 17454.85679807
                },
                {
                    "price": 0.566,
                    "qty": 18097.1547
                },
                {
                    "price": 0.5659,
                    "qty": 33644.89175666
                },
                {
                    "price": 0.5658,
                    "qty": 148.3464
                },
                {
                    "price": 0.5657,
                    "qty": 606.70854372
                }
            ],
            "asks": [
                {
                    "price": 0.5668,
                    "qty": 4410.79769741
                },
                {
                    "price": 0.5669,
                    "qty": 4655.40412487
                },
                {
                    "price": 0.567,
                    "qty": 49844.89424998
                },
                {
                    "price": 0.5671,
                    "qty": 24306.41678
                },
                {
                    "price": 0.5672,
                    "qty": 29783.25223475
                },
                {
                    "price": 0.5673,
                    "qty": 57234.71239278
                },
                {
                    "price": 0.5674,
                    "qty": 45065.04744
                },
                {
                    "price": 0.5675,
                    "qty": 5912.76380354
                },
                {
                    "price": 0.5676,
                    "qty": 42514.92434778
                },
                {
                    "price": 0.5677,
                    "qty": 36304.0847022
                }
            ],
            "checksum": 2439117997
        }
    ]
}`, "symbol", "Kraken", "book", "MATIC/USD"},
		{`{
    "channel": "book",
    "type": "update",
    "data": [
        {
            "symbol": "MATIC/USD",
            "bids": [
                {
                    "price": 0.5657,
                    "qty": 1098.3947558
                }
            ],
            "asks": [],
            "checksum": 2114181697,
            "timestamp": "2023-10-06T17:35:55.440295Z"
        }
    ]
}`, "symbol", "Kraken", "book", "MATIC/USD"},
	}

	for _, tc := range tests {
		symbol, normalized, err := utils.Normalize([]byte(tc.raw), tc.symbolKey, tc.feed, tc.channel)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		var m map[string]interface{}
		json.Unmarshal(normalized, &m)

		if string(symbol) != tc.symbol {
			t.Fatalf("expected symbol %v, got %v", tc.symbolKey, string(symbol))
		}

		if m["exchange"] != tc.feed {
			t.Fatalf("expected exchange %v, got %v", tc.feed, m["exchange"])
		}

		if m["channel"] != tc.channel {
			t.Fatalf("expected channel %v, got %v", tc.channel, m["channel"])
		}

	}

}
