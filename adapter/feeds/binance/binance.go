package binance

import (
	"market-adapter/constants"
	"market-adapter/feeds/utils"
	"market-adapter/logger"
	"sync"

	"github.com/gorilla/websocket"
)

type BinanceNormalizer struct {
	Channel string
}

type BinanceSubscriber struct {
	Channel    string
	ProductIds []string
}
type BinancePinger struct{}

const (
	Binance         string = "Binance"
	AggTradeChannel string = "aggTrade"
	SymbolField     string = "s"
)

type BinanceSubscribeMessage struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	Id     int      `json:"id"`
}

/*
sample response for agg trades:

	{
	  "e": "aggTrade",  // Event type
	  "E": 123456789,   // Event time
	  "s": "BTCUSDT",    // Symbol
	  "a": 5933014,		// Aggregate trade ID
	  "p": "0.001",     // Price
	  "q": "100",       // Quantity
	  "f": 100,         // First trade ID
	  "l": 105,         // Last trade ID
	  "T": 123456785,   // Trade time
	  "m": true,        // Is the buyer the market maker?
	}

sample response for depth:
	{
	"e": "depthUpdate", // Event type
	"E": 123456789,     // Event time
	"T": 123456788,     // Transaction time
	"s": "BTCUSDT",     // Symbol
	"U": 157,           // First update ID in event
	"u": 160,           // Final update ID in event
	"pu": 149,          // Final update Id in last stream(ie `u` in last stream)
	"b": [              // Bids to be updated
		[
		"0.0024",       // Price level to be updated
		"10"            // Quantity
		]
	],
	"a": [              // Asks to be updated
		[
		"0.0026",       // Price level to be updated
		"100"          // Quantity
		]
	]
	}
*/

type BinanceFactory struct{}

func (b *BinanceFactory) CreateNormalizer(channel string) (constants.Normalizer, error) {
	return &BinanceNormalizer{Channel: channel}, nil
}

func (b *BinanceFactory) CreateSubscriber(channel string, productIds []string) constants.Subscriber {
	return &BinanceSubscriber{Channel: channel, ProductIds: productIds}
}

func (b *BinanceFactory) CreatePinger() constants.Pinger {
	return &BinancePinger{}
}

func (b *BinanceNormalizer) Normalize(raw []byte) ([]byte, []byte, error) {
	return utils.Normalize(raw, SymbolField, Binance, b.Channel)
}

// subscribe message logic
func (b *BinanceSubscriber) Subscribe(conn *websocket.Conn) error {

	// multiple symbols for a stream per connection
	var channels []string
	for _, id := range b.ProductIds {
		channels = append(channels, id+"@"+b.Channel)
	}

	subscribeMsg := BinanceSubscribeMessage{
		Method: "SUBSCRIBE",
		Params: channels,
		Id:     1}

	// resp can be {"result": null, "id": 1}
	// or
	// {"code": 400, "msg": "Bad request"} - error case
	var okResp struct {
		Result interface{} `json:"result"`
		Id     int         `json:"id"`
		Code   int         `json:"code"`
		Msg    string      `json:"msg"`
	}

	err := utils.SendAndAckSubscribe(conn, subscribeMsg, &okResp, Binance, b.Channel)
	if err != nil {
		return logger.LogAndWrap(err.Error(), err, "feed", Binance, "channel", b.Channel)
	}

	// ERROR CASE
	if okResp.Code != 0 {
		return logger.LogAndWrap("Got error response from Binance upon subscription", nil, "code", okResp.Code, "msg", okResp.Msg)
	}

	logger.Log.Info("Successfully subscribed to Binance stream", "channel", b.Channel)

	return nil
}

// ping logic
func (b *BinancePinger) Ping(conn *websocket.Conn, mu *sync.Mutex) error {
	return utils.SendPing(conn, mu, Binance)
}
