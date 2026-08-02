package utils

import (
	"encoding/json"
	"shared/logger"
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
	var msg map[string]json.RawMessage
	var symbol string
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, nil, err
	}

	// binance, coinbase case
	if val, ok := msg[symbolKey]; ok {
		_ = json.Unmarshal(val, &symbol)
	} else {
		if dataRaw, ok := msg["data"]; ok {
			var dataArr []json.RawMessage
			if err := json.Unmarshal(dataRaw, &dataArr); err == nil && len(dataArr) > 0 {
				var firstObj map[string]json.RawMessage
				if err := json.Unmarshal(dataArr[0], &firstObj); err == nil {
					if val, ok := firstObj[symbolKey]; ok {
						_ = json.Unmarshal(val, &symbol)
					}
				}
			}
		}
	}

	if symbol == "" {
		return nil, nil, logger.LogAndWrap("Unable to locate symbol in ticker message", nil, "feed", feed, "channel", channel, "msg", msg)
	}

	// add in the root level for kafka consumer processing
	exchangeJson, _ := json.Marshal(feed)
	channelJson, _ := json.Marshal(channel)
	symbolJson, _ := json.Marshal(symbol)
	msg[ExchangeField] = exchangeJson
	msg[ChannelField] = channelJson
	msg[symbolKey] = symbolJson

	normalized, marshalErr := json.Marshal(msg)
	if marshalErr != nil {
		logger.Log.Error("Error in marshalling normalized trade message", "feed", feed, "channel", channel, "error", marshalErr)
		return nil, nil, marshalErr
	}

	return []byte(symbol), normalized, nil
}
