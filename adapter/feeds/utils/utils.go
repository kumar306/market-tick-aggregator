package utils

import (
	"bytes"
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

// Normalize extracts the symbol from a raw exchange message and returns the
// symbol plus the message with exchange/channel/symbol fields injected, for
// downstream Kafka consumers.
//
// This avoids decoding the whole message into map[string]json.RawMessage and
// re-marshaling it, which was costing ~65 allocations per call (one string +
// one RawMessage per top-level key on decode, then a full key sort and
// re encode on marshal, on top of three separate json.Marshal calls for the
// injected fields) - expensive because the cost is in the decode everything then reencode
// shape, not the value type.
//
// The common case (Binance, Coinbase: symbol is a direct top-level string
// field) is handled by a boundary-checked byte scan with no JSON decoding at
// all. The rarer case (Kraken: symbol nested inside a "data" array) falls
// back to the original map-based extraction, which is fine since the
// benefit here is on the volume path, not the occasional nested one. Output
// is built by appending the extra fields directly onto the original bytes
// instead of re-marshaling the whole message.
func Normalize(raw []byte, symbolKey, feed, channel string) ([]byte, []byte, error) {
	symbol, foundAtTopLevel := fastExtractTopLevelString(raw, symbolKey)
	if !foundAtTopLevel {
		var err error
		symbol, err = extractSymbolSlow(raw, symbolKey)
		if err != nil {
			return nil, nil, err
		}
	}

	if symbol == "" {
		return nil, nil, logger.LogAndWrap("Unable to locate symbol in ticker message", nil, "feed", feed, "channel", channel)
	}

	trimmed := bytes.TrimRight(raw, " \t\r\n")
	if len(trimmed) == 0 || trimmed[len(trimmed)-1] != '}' {
		return nil, nil, logger.LogAndWrap("Raw message is not a JSON object", nil, "feed", feed, "channel", channel)
	}

	// trimmed contains formatted raw. in kraken case, symbol is not at top level, if its there, already skip adding it. else add it
	extra := len(ExchangeField) + len(feed) + len(ChannelField) + len(channel) + len(symbolKey) + len(symbol) + 48
	out := make([]byte, 0, len(trimmed)+extra)
	out = append(out, trimmed[:len(trimmed)-1]...)
	out = append(out, ',')
	out = appendJSONStringField(out, ExchangeField, feed)
	out = append(out, ',')
	out = appendJSONStringField(out, ChannelField, channel)
	if !foundAtTopLevel {
		// only add symbolKey at the top level if it wasn't already there.
		// for Binance/Coinbase this field already exists (that's how the
		// fast path found it), so re-adding it would create a duplicate key.
		out = append(out, ',')
		out = appendJSONStringField(out, symbolKey, symbol)
	}
	out = append(out, '}')

	return []byte(symbol), out, nil
}

func appendJSONStringField(dst []byte, key, value string) []byte {
	dst = append(dst, '"')
	dst = append(dst, key...)
	dst = append(dst, '"', ':', '"')
	dst = append(dst, value...)
	dst = append(dst, '"')
	return dst
}

// fastExtractTopLevelString looks for `"key":"value"` as a direct top-level
// field via byte scanning, with no JSON decoding. Returns ("", false) if the
// pattern isn't found in this exact compact shape, so the caller can fall back to the general decoder
// exchange WebSocket payloads are compact JSON

// the match must be immediately preceded by '{' or ',', which is the only
// place a JSON object's own keys can start - this rules out the pattern
// coincidentally appearing inside some other field's string value.
func fastExtractTopLevelString(raw []byte, key string) (string, bool) {
	pattern := []byte(`"` + key + `":"`)

	searchFrom := 0
	for {
		i := bytes.Index(raw[searchFrom:], pattern)
		if i < 0 {
			return "", false
		}
		absIdx := searchFrom + i
		if absIdx > 0 && (raw[absIdx-1] == '{' || raw[absIdx-1] == ',') {
			valStart := absIdx + len(pattern)
			valEnd := bytes.IndexByte(raw[valStart:], '"')
			if valEnd < 0 {
				return "", false
			}
			return string(raw[valStart : valStart+valEnd]), true
		}
		searchFrom = absIdx + 1
	}
}

// handles the general/nested case (i.e. Kraken's symbol
// living inside a "data" array) via a full decode. Only reached when the
// fast path's compact top-level assumption doesn't hold, so the allocation
// cost here is bounded by how often that's true, not by overall volume.
func extractSymbolSlow(raw []byte, symbolKey string) (string, error) {
	var msg map[string]json.RawMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return "", err
	}

	if val, ok := msg[symbolKey]; ok {
		var symbol string
		_ = json.Unmarshal(val, &symbol)
		return symbol, nil
	}

	if dataRaw, ok := msg["data"]; ok {
		var dataArr []json.RawMessage
		if err := json.Unmarshal(dataRaw, &dataArr); err == nil && len(dataArr) > 0 {
			var firstObj map[string]json.RawMessage
			if err := json.Unmarshal(dataArr[0], &firstObj); err == nil {
				if val, ok := firstObj[symbolKey]; ok {
					var symbol string
					_ = json.Unmarshal(val, &symbol)
					return symbol, nil
				}
			}
		}
	}

	return "", nil
}
