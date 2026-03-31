package internal

import (
	"context"
	"market-adapter/constants"
	"market-adapter/ring"
	"shared/logger"
	"shared/metrics"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

func ReadMessages(conn *websocket.Conn, ctx context.Context, wg *sync.WaitGroup, ring *ring.SpscDropOldestRing[[]byte]) {
	name := ring.Name
	metrics.Adapter_SupervisorGoroutines.WithLabelValues(name).Inc()
	defer wg.Done()
	defer metrics.Adapter_SupervisorGoroutines.WithLabelValues(name).Dec()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, msg, err := conn.ReadMessage()
			if err != nil {
				logger.Log.Error("Failed to read message for feed", "name", name, "err", err)
				continue
			}

			ring.Push(msg)

		}
	}
}

func SendHeartbeat(conn *websocket.Conn,
	ctx context.Context,
	wg *sync.WaitGroup,
	handler *constants.StreamHandler,
	ticker *time.Ticker,
	name string) {
	metrics.Adapter_SupervisorGoroutines.WithLabelValues(name).Inc()
	defer wg.Done()
	defer metrics.Adapter_SupervisorGoroutines.WithLabelValues(name).Dec()
	for {
		select {
		case <-ticker.C:
			handler.Pinger.Ping(conn, handler.Mu)
		case <-ctx.Done():
			return
		}
	}
}

func MonitorConnection(
	supervisor *constants.Supervisor,
	streamCfg *constants.Stream,
	ticker *time.Ticker) {
	metrics.Adapter_SupervisorGoroutines.WithLabelValues(streamCfg.Name).Inc()
	defer supervisor.Wg.Done()
	defer metrics.Adapter_SupervisorGoroutines.WithLabelValues(streamCfg.Name).Dec()
	for {
		select {
		case <-ticker.C:
			if time.Since(supervisor.LastPongTime) > time.Duration(streamCfg.PongTimeout)*time.Second {
				logger.Log.Warn("Pong timeout -- cancelling the connection", "name", streamCfg.Name)
				supervisor.Cancel()
				return
			}

		case <-supervisor.Ctx.Done():
			return
		}
	}
}
