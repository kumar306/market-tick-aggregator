package binance

import (
	"encoding/json"
	"market-adapter/logger"
	"time"

	"github.com/gorilla/websocket"
)

type BinanceNormalizer struct{}
type BinanceSubscriber struct{}
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
// TODO: make the streams taken from config
func (b *BinanceSubscriber) Subscribe(conn *websocket.Conn) error {
	subscribeMsg := BinanceSubscribeMessage{
		Method: "SUBSCRIBE",
		Params: []string{"btcusdt@aggTrade", "btcusdt@depth"},
		Id:     1}

	subscribeJson, err := json.Marshal(subscribeMsg)
	if err != nil {
		return logger.LogAndWrap("Error creating subscribe message for binance", err, "feed", "binance")
	}

	err = conn.WriteJSON(subscribeJson)
	if err != nil {
		return logger.LogAndWrap("Error subscribing to binance", err, "feed", "binance")
	}

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
