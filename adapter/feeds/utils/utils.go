package utils

import (
	"encoding/json"
	"market-adapter/logger"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	ExchangeField string = "exchange"
	ChannelField  string = "channel"
)

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

func Normalize(raw []byte, symbolKey, feed, channel string) ([]byte, []byte, error) {
	var msg map[string]interface{}
	var symbol string
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, nil, err
	}

	// binance, coinbase case
	if val, ok := msg[symbolKey]; ok {
		symbol, _ = val.(string)
	} else {
		// kraken case - symbol inside data []
		if dataArr, ok := msg["data"].([]interface{}); ok && len(dataArr) > 0 {
			if firstObj, ok := dataArr[0].(map[string]interface{}); ok {
				if val, ok := firstObj[symbolKey]; ok {
					symbol, _ = val.(string)
				}
			}
		}
	}

	if symbol == "" {
		return nil, nil, logger.LogAndWrap("Unable to locate symbol in ticker message", nil, "feed", feed, "channel", channel)
	}

	// add in the root level for kafka consumer processing
	msg[ExchangeField] = feed
	msg[ChannelField] = channel
	msg[symbolKey] = symbol

	normalized, marshalErr := json.Marshal(msg)
	if marshalErr != nil {
		logger.Log.Error("Error in marshalling normalized trade message", "feed", feed, "channel", channel, "error", marshalErr)
		return nil, nil, marshalErr
	}

	logger.Log.Info("Normalized trade response for message", "name", feed, "symbol", symbol)

	return []byte(symbol), normalized, nil
}
