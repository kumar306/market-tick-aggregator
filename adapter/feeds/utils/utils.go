package utils

import (
	"encoding/json"
	"market-adapter/logger"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const ExchangeField string = "Exchange"

func SendAndAckSubscribe[T any](conn *websocket.Conn, subscribeReq interface{}, subscribeRes *T, feed string, channel string) error {
	subscribeJson, err := json.Marshal(subscribeReq)
	if err != nil {
		return logger.LogAndWrap("Error creating subscribe message", err, "feed", feed, "stream", channel)
	}

	err = conn.WriteMessage(websocket.TextMessage, subscribeJson)
	if err != nil {
		return logger.LogAndWrap("Error writing subscribe message to connection", err, "feed", feed, "stream", channel)
	}

	_, msg, readErr := conn.ReadMessage()
	if readErr != nil {
		return logger.LogAndWrap("Error reading subscribe acknowledgement in connection", readErr, "feed", feed, "stream", channel)
	}

	if err = json.Unmarshal(msg, subscribeRes); err != nil {
		return logger.LogAndWrap("Error in parsing subscribe response", err, "feed", feed, "stream", channel)
	}

	return nil
}

func SendPing(conn *websocket.Conn, mu *sync.Mutex, feed string) error {
	mu.Lock()
	err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second*5))
	mu.Unlock()
	if err != nil {
		return logger.LogAndWrap("Error when writing ping message", err, "feed", feed)
	}
	return nil
}

func NormalizeTrade(raw []byte, symbolKey, feed, channel string) ([]byte, []byte, error) {
	var data map[string]interface{}

	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, nil, err
	}

	symbol, ok := data[symbolKey].(string)
	if !ok || symbol == "" {
		return nil, nil, logger.LogAndWrap("Symbol not present in trade msg", nil, "feed", feed, "channel", channel, "symbol", symbol)
	}

	data[ExchangeField] = feed

	normalized, marshalErr := json.Marshal(data)
	if marshalErr != nil {
		logger.Log.Error("Error in marshalling normalized trade message", "feed", feed, "channel", channel, "error", marshalErr)
		return nil, nil, marshalErr
	}

	logger.Log.Info("Normalized trade response for message", "name", feed, "symbol", symbol)

	return []byte(symbol), normalized, nil
}
