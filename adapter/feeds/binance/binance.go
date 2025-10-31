package binance

import (
	"encoding/json"
	"market-adapter/logger"
	"time"

	"github.com/gorilla/websocket"
)

type BinanceAggTradeNormalizer struct{}
type BinanceDepthNormalizer struct{}

type BinanceSubscriber struct {
	Channel string
}
type BinancePinger struct{}

const Binance string = "Binance"

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
	var aggTradeMsg BinanceAggTradeMessage
	err := json.Unmarshal(raw, &aggTradeMsg)
	if err != nil {
		logger.Log.Error("Error in parsing agg trades binance response", "feed", "binance", "channel", "aggTrades", "error", err)
		return nil, nil, err
	}

	aggTradeMsg.Exchange = Binance
	symbol := aggTradeMsg.Symbol

	normalized, marshalErr := json.Marshal(aggTradeMsg)
	if marshalErr != nil {
		logger.Log.Error("Error in marshalling normalized agg trade message", "feed", "binance", "channel", "aggTrades", "error", marshalErr)
		return nil, nil, err
	}

	return []byte(symbol), normalized, nil
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

	return []byte(symbol), normalized, nil
}

// subscribe message logic
func (b *BinanceSubscriber) Subscribe(conn *websocket.Conn) error {
	subscribeMsg := BinanceSubscribeMessage{
		Method: "SUBSCRIBE",
		Params: []string{b.Channel},
		Id:     1}

	subscribeJson, err := json.Marshal(subscribeMsg)
	if err != nil {
		return logger.LogAndWrap("Error creating subscribe message for binance", err, "feed", "binance")
	}

	err = conn.WriteMessage(websocket.TextMessage, subscribeJson)
	if err != nil {
		return logger.LogAndWrap("Error writing subscribe message to binance connection", err, "feed", "binance", "stream", b.Channel)
	}

	_, msg, readErr := conn.ReadMessage()
	if readErr != nil {
		return logger.LogAndWrap("Error reading subscribe acknowledgement in binance connection", err, "feed", "binance", "stream", b.Channel)
	}

	// resp can be {"result": null, "id": 1}
	// or
	// {"code": 400, "msg": "Bad request"} - error case
	var okResp struct {
		Result interface{} `json:"result"`
		Id     int         `json:"id"`
		Code   int         `json:"code"`
		Msg    string      `json:"msg"`
	}

	if err = json.Unmarshal(msg, &okResp); err != nil {
		return logger.LogAndWrap("Error in parsing subscribe response for Binance", err, "feed_name", "binance", "stream", b.Channel)
	}

	// ERROR CASE
	if okResp.Code != 0 {
		return logger.LogAndWrap("Got error response from Binance upon subscription", nil, "code", okResp.Code, "msg", okResp.Msg)
	}

	logger.Log.Info("Successfully subscribed to Binance stream", "stream_channel", b.Channel)

	return nil
}

// ping logic
func (b *BinancePinger) Ping(conn *websocket.Conn) error {
	err := conn.WriteControl(websocket.PongMessage, []byte{}, time.Now().Add(time.Second*5))
	if err != nil {
		return logger.LogAndWrap("Error when writing pong message to binance", err, "feed", "binance")
	}
	return nil
}
