package binance

import (
	"encoding/json"
	"market-adapter/logger"
	"time"

	"github.com/gorilla/websocket"
)

type BinanceNormalizer struct{}
type BinanceSubscriber struct {
	Channel string
}
type BinancePinger struct{}

type BinanceSubscribeMessage struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	Id     int      `json:"id"`
}

func (b *BinanceNormalizer) Normalize([]byte) (string, []byte, error) {
	return "", nil, nil
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
