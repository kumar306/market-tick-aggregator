package kraken

import (
	"encoding/json"
	"market-adapter/constants"
	"market-adapter/feeds/utils"
	"shared/logger"

	"sync"

	"github.com/gorilla/websocket"
)

/* subscribe req:
{
    "method": "subscribe",
    "params": {
        "channel": "ticker",
        "symbol": [
            "ALGO/USD"
        ]
    }
}
response:
{
    "method": "subscribe",
    "result": {
        "channel": "ticker",
        "snapshot": true,
        "symbol": "ALGO/USD"
    },
    "success": true,
    "time_in": "2023-09-25T09:04:31.742599Z",
    "time_out": "2023-09-25T09:04:31.742648Z"
}

book sample response (took from api docs):
{
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
                }
			],
			"asks": [
			 {
                    "price": 0.5666,
                    "qty": 4831.75496356
                },
                {
                    "price": 0.5665,
                    "qty": 6658.22734739
                }
			],
			"checksum": 32312314
		}
	]
}
*/

type KrakenFactory struct{}

const (
	SubscribeType string = "subscribe"
	KrakenType    string = "kraken"
	TickerType    string = "ticker"
	BookType      string = "book"
	SymbolField   string = "symbol"
)

type KrakenSubscribeRequest struct {
	Method string               `json:"method"`
	Params KrakenSubscribeParam `json:"params"`
}

type KrakenSubscribeParam struct {
	Channel string   `json:"channel"`
	Symbol  []string `json:"symbol"`
}

type KrakenSubscribeResponse struct {
	Method  string `json:"method"`
	Success bool   `json:"success"`
}

type KrakenNormalizer struct {
	Channel string
}

type KrakenSubscriber struct {
	Channel    string
	ProductIds []string
}
type KrakenPinger struct{}

func (k *KrakenSubscriber) Subscribe(conn *websocket.Conn) error {
	var krakenSubscribeRequest KrakenSubscribeRequest = KrakenSubscribeRequest{
		Method: SubscribeType,
		Params: KrakenSubscribeParam{
			Channel: k.Channel,
			Symbol:  k.ProductIds,
		},
	}
	var KrakenSubscribeResponse KrakenSubscribeResponse
	err := utils.SendAndAckSubscribe(conn, krakenSubscribeRequest, &KrakenSubscribeResponse, KrakenType, k.Channel)
	if err != nil {
		return logger.LogAndWrap(err.Error(), err, "feed", KrakenType, "channel", k.Channel)
	}

	return nil
}

func (k *KrakenNormalizer) Normalize(raw []byte) ([]byte, []byte, error) {
	if shouldSkipControlMessage(raw) {
		return nil, nil, nil
	}

	return utils.Normalize(raw, SymbolField, KrakenType, k.Channel)
}

func (k *KrakenPinger) Ping(conn *websocket.Conn, mu *sync.Mutex) error {
	return utils.SendPing(conn, mu, KrakenType)
}

func (k *KrakenFactory) CreateNormalizer(channel string) (constants.Normalizer, error) {
	return &KrakenNormalizer{Channel: channel}, nil
}

func (k *KrakenFactory) CreateSubscriber(channel string, productIds []string) constants.Subscriber {
	return &KrakenSubscriber{
		Channel:    channel,
		ProductIds: productIds,
	}
}

func (k *KrakenFactory) CreatePinger() constants.Pinger {
	return &KrakenPinger{}
}

func shouldSkipControlMessage(raw []byte) bool {
	var msg map[string]interface{}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return false
	}

	channel, _ := msg["channel"].(string)
	if channel == "heartbeat" {
		return true
	}

	method, _ := msg["method"].(string)
	return method == SubscribeType
}
