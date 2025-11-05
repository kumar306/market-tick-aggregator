package coinbase

import (
	"encoding/json"
	"market-adapter/constants"
	"market-adapter/feeds/utils"
	"market-adapter/logger"
	"sync"

	"github.com/gorilla/websocket"
)

/*
	sample coinbase ticker message
	{
		"type": "ticker",
		"sequence": 37475248783,
		"product_id": "ETH-USD",
		"price": "1285.22",
		"open_24h": "1310.79",
		"volume_24h": "245532.79269678",
		"low_24h": "1280.52",
		"high_24h": "1313.8",
		"volume_30d": "9788783.60117027",
		"best_bid": "1285.04",
		"best_bid_size": "0.46688654",
		"best_ask": "1285.27",
		"best_ask_size": "1.56637040",
		"side": "buy",
		"time": "2022-10-19T23:28:22.061769Z",
		"trade_id": 370843401,
		"last_size": "11.4396987"
	}

	sample l2 response:
	{
		"type": "snapshot",
		"product_id": "BTC-USD",
		"bids": [["10101.10", "0.45054140"]],
		"asks": [["10102.55", "0.57753524"]]
	}

	l2updates:
	{
		"type": "l2update",
		"product_id": "BTC-USD",
		"changes": [
			[
			"buy",
			"22356.270000",
			"0.00000000"
			],
			[
			"buy",
			"22356.300000",
			"1.00000000"
			]
		],
		"time": "2022-08-04T15:25:05.010758Z"
	}

	sample subscribe request:
	{
		"type": "subscribe",
		"product_ids": [
			"ETH-USD",
			"BTC-USD"
		],
		"channels": ["level2_batch"]
	}

	sample subscribe success response:
	// Response
	{
		"type": "subscriptions",
		"channels": [
			{
			"name": "level2",
			"product_ids": ["ETH-USD", "ETH-EUR"]
			},
			{
			"name": "heartbeat",
			"product_ids": ["ETH-USD", "ETH-EUR"]
			},
			{
			"name": "ticker",
			"product_ids": ["ETH-USD", "ETH-EUR", "ETH-BTC"]
			}
		]
	}
*/

type CBFactory struct{}

const (
	SubscribeType     string = "subscribe"
	CoinbaseType      string = "Coinbase"
	SnapshotType      string = "snapshot"
	UpdateType        string = "l2update"
	SubscriptionsType string = "subscriptions"
	TickerType        string = "ticker"
	Level2Type        string = "level2"
)

type CBSubscribeRequest struct {
	Type       string   `json:"type"`
	ProductIds []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

type CBSubscribeResponse struct {
	Type     string        `json:"type"`
	Channels []ChannelInfo `json:"channels"`
}

type ChannelInfo struct {
	Name       string   `json:"name"`
	ProductIds []string `json:"product_ids"`
}

type CBSubscriber struct {
	Channel    string
	ProductIds []string
}

type CBTickerMessage struct {
	Exchange    string
	Type        string `json:"type"`
	Sequence    int64  `json:"sequence"`
	ProductID   string `json:"product_id"`
	Price       string `json:"price"`
	Open24h     string `json:"open_24h"`
	Volume24h   string `json:"volume_24h"`
	Low24h      string `json:"low_24h"`
	High24h     string `json:"high_24h"`
	Volume30d   string `json:"volume_30d"`
	BestBid     string `json:"best_bid"`
	BestBidSize string `json:"best_bid_size"`
	BestAsk     string `json:"best_ask"`
	BestAskSize string `json:"best_ask_size"`
	Side        string `json:"side"`
	Time        string `json:"time"`
	TradeID     int64  `json:"trade_id"`
	LastSize    string `json:"last_size"`
}

type CBL2Snapshot struct {
	Exchange  string
	Type      string      `json:"type"`
	ProductId string      `json:"product_id"`
	Bids      [][2]string `json:"bids"`
	Asks      [][2]string `json:"asks"`
}

type CBL2Update struct {
	Exchange  string
	Type      string      `json:"type"`
	ProductId string      `json:"product_id"`
	Changes   [][3]string `json:"changes"`
	Time      string      `json:"time"`
}

type CBTickerNormalizer struct{}

type CBL2Normalizer struct{}

type CBPinger struct{}

// for ticker channel
func (c *CBTickerNormalizer) Normalize(raw []byte) ([]byte, []byte, error) {
	return utils.NormalizeTrade(raw, "product_id", CoinbaseType, TickerType)

}

func (c *CBL2Normalizer) Normalize(raw []byte) ([]byte, []byte, error) {
	// first, we get a snapshot message. then we get a snapshot update message.
	var msgType struct {
		Type string `json:"type"`
	}

	parseErr := json.Unmarshal(raw, &msgType)
	if parseErr != nil {
		logger.Log.Error("Error in parse L2 message", "name", CoinbaseType, "channel", "l2", "error", parseErr)
		return nil, nil, parseErr
	}

	switch msgType.Type {
	case SnapshotType:
		var snapshot CBL2Snapshot
		json.Unmarshal(raw, &snapshot)
		snapshot.Exchange = CoinbaseType
		normalized, err := json.Marshal(snapshot)
		if err != nil {
			logger.Log.Error("Error in normalize L2 shapshot message", "name", CoinbaseType, "channel", "l2", "error", err)
			return nil, nil, err
		}

		logger.Log.Info("Normalized L2 snapshot message", "name", CoinbaseType, "channel", "l2")
		return []byte(snapshot.ProductId), normalized, nil

	case UpdateType:
		var L2Update CBL2Update
		json.Unmarshal(raw, &L2Update)
		L2Update.Exchange = CoinbaseType
		normalized, err := json.Marshal(L2Update)
		if err != nil {
			logger.Log.Error("Error in normalize L2 update message", "name", CoinbaseType, "channel", "l2", "error", err)
			return nil, nil, err
		}

		logger.Log.Info("Normalized L2 update message", "name", CoinbaseType, "channel", "l2")
		return []byte(L2Update.ProductId), normalized, nil

	default:
		return nil, nil, logger.LogAndWrap("No matching type for coinbase L2 message", nil, "name", CoinbaseType, "channel", "l2")
	}

}

// create the req, write it. read ack. if any errors dont proceed.
func (c *CBSubscriber) Subscribe(conn *websocket.Conn) error {
	var CBSubscribeRequest CBSubscribeRequest = CBSubscribeRequest{
		Type:       SubscribeType,
		ProductIds: c.ProductIds,
		Channels:   []string{c.Channel},
	}

	var CBSubscribeResponse CBSubscribeResponse

	err := utils.SendAndAckSubscribe(conn, CBSubscribeRequest, &CBSubscribeResponse, CoinbaseType, c.Channel)
	if err != nil {
		return logger.LogAndWrap("Error when writing ping message to coinbase", err, "feed", CoinbaseType)
	}

	logger.Log.Info("Successfully subscribed to Coinbase", "channel", c.Channel, "productIds", c.ProductIds)

	return nil
}

func (c *CBPinger) Ping(conn *websocket.Conn, mu *sync.Mutex) error {
	return utils.SendPing(conn, mu, CoinbaseType)
}

func (c *CBFactory) CreateNormalizer(channel string) (constants.Normalizer, error) {
	switch channel {
	case TickerType:
		return &CBTickerNormalizer{}, nil
	case Level2Type:
		return &CBL2Normalizer{}, nil
	default:
		return nil, logger.LogAndWrap("Unsupported channel type for normalizer", nil, "name", CoinbaseType, "channel", channel)
	}
}

func (c *CBFactory) CreateSubscriber(channel string, productIds []string) constants.Subscriber {
	return &CBSubscriber{Channel: channel, ProductIds: productIds}
}

func (c *CBFactory) CreatePinger() constants.Pinger {
	return &CBPinger{}
}
