package controller

import (
	"encoding/json"
	"market-ui-backend/stream"
	"net/http"
	"shared/logger"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type SubscribeMessage struct {
	Type     string `json:"type"`
	Exchange string `json:"exchange"`
	Symbol   string `json:"symbol"`
}

func HandleWebSocket(ctx *gin.Context) {
	conn, err := upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		logger.Log.Error("Error in upgrading websocket connection", "error", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// create the client and start
	client := &stream.WSClient{
		In:   make(chan []byte, 256),
		Conn: conn,
	}

	go HandleWrite(client)
	HandleSubscribe(client)
}

func HandleWrite(client *stream.WSClient) {
	for msg := range client.In {
		if writeErr := client.Conn.WriteMessage(websocket.TextMessage, msg); writeErr != nil {
			logger.Log.Info("Error in writing message", "error", writeErr)
			break
		}
	}
}

func HandleSubscribe(client *stream.WSClient) {
	defer func() {
		stream.Manager.Unregister(client)
		client.Conn.Close()
	}()

	for {
		_, msg, err := client.Conn.ReadMessage()
		if err != nil {
			logger.Log.Error("Read error", "error", err)

			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				logger.Log.Info("Connection closed", "error", err)
				break
			}
			continue
		}

		logger.Log.Info("Received message", "message", string(msg))
		var sub SubscribeMessage

		jsonErr := json.Unmarshal(msg, &sub)
		if jsonErr != nil {
			logger.Log.Error("Unable to parse subscription request. Enter again", "error", jsonErr)
			continue
		}

		if sub.Type != "tick" && sub.Type != "book" {
			logger.Log.Error("Invalid subscribe type. Expected tick or book")
			continue
		}

		// now got exchange symbol from sub message. need to register. before this need a stream manager
		key := sub.Exchange + ":" + sub.Symbol
		stream.Manager.Register(sub.Type, key, client)
	}
}
