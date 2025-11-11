package coinbase

import (
	"market-adapter/feeds/utils"
	"shared/constants"
	"shared/logger"

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
	SymbolField       string = "product_id"
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

type CBNormalizer struct {
	Channel string
}

type CBPinger struct{}

func (c *CBNormalizer) Normalize(raw []byte) ([]byte, []byte, error) {
	return utils.Normalize(raw, SymbolField, CoinbaseType, c.Channel)
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
	return &CBNormalizer{Channel: channel}, nil
}

func (c *CBFactory) CreateSubscriber(channel string, productIds []string) constants.Subscriber {
	return &CBSubscriber{Channel: channel, ProductIds: productIds}
}

func (c *CBFactory) CreatePinger() constants.Pinger {
	return &CBPinger{}
}
