package stream

import (
	"shared/logger"
	"sync"

	"github.com/gorilla/websocket"
)

type StreamManager struct {
	mu      sync.RWMutex
	Clients map[string]map[string]map[*WSClient]struct{}
}

type WSClient struct {
	Type string // tick or book
	Key  string // exchange:symbol
	Conn *websocket.Conn
	In   chan []byte // client's channel which reads from kafka
}

var Manager *StreamManager = NewStreamManager()

func NewStreamManager() *StreamManager {
	return &StreamManager{
		Clients: make(map[string]map[string]map[*WSClient]struct{}),
	}
}

func (sm *StreamManager) Register(msgType string, key string, client *WSClient) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	client.Type = msgType
	client.Key = key

	if sm.Clients[msgType] == nil {
		sm.Clients[msgType] = make(map[string]map[*WSClient]struct{})
	}

	if sm.Clients[msgType][key] == nil {
		sm.Clients[msgType][key] = make(map[*WSClient]struct{})
	}

	sm.Clients[msgType][key][client] = struct{}{}
	logger.Log.Info("Registered new client for key", "key", key)
}

func (sm *StreamManager) Unregister(client *WSClient) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for typ, clientMap := range sm.Clients {
		for key, clients := range clientMap {
			delete(clients, client)
			if len(clients) == 0 {
				delete(clientMap, key)
			}
		}
		if len(clientMap) == 0 {
			delete(sm.Clients, typ)
		}
	}

	logger.Log.Info("Unregistered successfully")
}

func (sm *StreamManager) Broadcast(msgType, key string, msg []byte) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if _, ok := sm.Clients[msgType][key]; !ok {
		logger.Log.Info("No connections present for key", "key", key)
		return
	}

	for cl := range sm.Clients[msgType][key] {
		select {
		case cl.In <- msg:
		default:
			logger.Log.Info("Dropping slow client")
		}
	}
}
