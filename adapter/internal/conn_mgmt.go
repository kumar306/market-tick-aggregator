package internal

import (
	"context"
	"market-adapter/constants"
	"market-adapter/metrics"
	"market-adapter/ring"
	"shared/logger"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

func ReadMessages(conn *websocket.Conn, ctx context.Context, wg *sync.WaitGroup, ring *ring.SpscDropOldestRing[[]byte]) {
	name := ring.Name
	metrics.SupervisorGoroutines.WithLabelValues(name).Inc()
	defer wg.Done()
	defer metrics.SupervisorGoroutines.WithLabelValues(name).Dec()
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Closing read loop. Returning", "name", name)
			return
		default:
			_, msg, err := conn.ReadMessage()
			if err != nil {
				logger.Log.Error("Failed to read message for feed", "name", name, "err", err)
				continue
			}

			ring.Push(msg)
			logger.Log.Info("Received message for feed", "name", name, "msg", string(msg))

		}
	}
}

func SendHeartbeat(conn *websocket.Conn,
	ctx context.Context,
	wg *sync.WaitGroup,
	handler *constants.StreamHandler,
	ticker *time.Ticker,
	name string) {
	metrics.SupervisorGoroutines.WithLabelValues(name).Inc()
	defer wg.Done()
	defer metrics.SupervisorGoroutines.WithLabelValues(name).Dec()
	for {
		select {
		case <-ticker.C:
			handler.Pinger.Ping(conn, handler.Mu)
		case <-ctx.Done():
			logger.Log.Info("Shutting down heartbeat loop. Returning", "name", name)
			return
		}
	}
}

func MonitorConnection(
	supervisor *constants.Supervisor,
	streamCfg *constants.Stream,
	ticker *time.Ticker) {
	metrics.SupervisorGoroutines.WithLabelValues(streamCfg.Name).Inc()
	defer supervisor.Wg.Done()
	defer metrics.SupervisorGoroutines.WithLabelValues(streamCfg.Name).Dec()
	for {
		select {
		case <-ticker.C:
			if time.Since(supervisor.LastPongTime) > time.Duration(streamCfg.PongTimeout)*time.Second {
				logger.Log.Warn("Pong timeout -- cancelling the connection", "name", streamCfg.Name)
				supervisor.Cancel()
				return
			}

		case <-supervisor.Ctx.Done():
			logger.Log.Info("Shutting down monitor loop", "name", streamCfg.Name)
			return
		}
	}
}
