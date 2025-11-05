package binance

import (
	"encoding/json"
	"market-adapter/feeds/utils"
	"market-adapter/logger"
	"sync"

	"github.com/gorilla/websocket"
)

type BinanceAggTradeNormalizer struct{}
type BinanceDepthNormalizer struct{}

type BinanceSubscriber struct {
	Channel    string
	ProductIds []string
}
type BinancePinger struct{}

const (
	Binance         string = "Binance"
	AggTradeChannel string = "aggTrade"
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
*/
type BinanceAggTradeMessage struct {
	Exchange         string
	EventType        string `json:"e"`
	EventTime        uint64 `json:"E"`
	Symbol           string `json:"s"`
	AggregateTradeId uint64 `json:"a"`
	Price            string `json:"p"`
	Quantity         string `json:"q"`
	FirstTradeId     uint64 `json:"f"`
	LastTradeId      uint64 `json:"l"`
	TradeTime        uint64 `json:"T"`
	IsMarketMaker    bool   `json:"m"`
}

/*
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

type BinanceDepthMessage struct {
	Exchange          string
	EventType         string     `json:"e"`
	EventTime         uint64     `json:"E"`
	TransactionTime   uint64     `json:"T"`
	Symbol            string     `json:"s"`
	FirstUpdateId     uint64     `json:"U"`
	FinalUpdateId     uint64     `json:"u"`
	PrevFinalUpdateId uint64     `json:"pu"`
	Bids              [][]string `json:"b"`
	Asks              [][]string `json:"a"`
}

func (b *BinanceAggTradeNormalizer) Normalize(raw []byte) ([]byte, []byte, error) {
	return utils.NormalizeTrade(raw, "s", Binance, AggTradeChannel)
}

func (b *BinanceDepthNormalizer) Normalize(raw []byte) ([]byte, []byte, error) {
	var depthMessage BinanceDepthMessage
	err := json.Unmarshal(raw, &depthMessage)
	if err != nil {
		logger.Log.Error("Error in parsing depth binance response", "feed", "binance", "channel", "depth", "error", err)
		return nil, nil, err
	}

	depthMessage.Exchange = Binance
	symbol := depthMessage.Symbol

	normalized, marshalErr := json.Marshal(depthMessage)
	if marshalErr != nil {
		logger.Log.Error("Error in marshalling normalized depth message", "feed", "binance", "channel", "depth", "error", marshalErr)
		return nil, nil, err
	}

	logger.Log.Info("Normalized depth response for message", "name", Binance, "symbol", symbol, "message", normalized)

	return []byte(symbol), normalized, nil
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
