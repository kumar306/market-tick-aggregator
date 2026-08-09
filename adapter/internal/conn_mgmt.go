package internal

import (
	"bytes"
	"context"
	"market-adapter/constants"
	"market-adapter/ring"
	"shared/logger"
	"shared/metrics"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

func ReadMessages(conn *websocket.Conn, ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, ring *ring.SpscDropOldestRing[[]byte]) {
	name := ring.Name
	metrics.Adapter_SupervisorGoroutines.WithLabelValues(name).Inc()
	defer wg.Done()
	defer metrics.Adapter_SupervisorGoroutines.WithLabelValues(name).Dec()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, r, err := conn.NextReader()
			if err != nil {
				logger.Log.Error("Failed to read message for feed", "name", name, "err", err)
				cancel()
				continue
			}

			// borrow a scratch buffer instead of io.ReadAll allocating a
			// fresh one per message (that was 17% of all adapter allocation
			// volume under load, per pprof). PublishToKafkaLoop hands this
			// back to the pool once Normalize() has copied everything it
			// needs out of it.
			bufPtr := messageBufferPool.Get().(*[]byte)
			buf := bytes.NewBuffer((*bufPtr)[:0])
			if _, err := buf.ReadFrom(r); err != nil {
				logger.Log.Error("Failed to read message for feed", "name", name, "err", err)
				messageBufferPool.Put(bufPtr)
				cancel()
				continue
			}

			ring.Push(buf.Bytes())
		}
	}
}

func SendHeartbeat(conn *websocket.Conn,
	ctx context.Context,
	cancel context.CancelFunc,
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
			if err := handler.Pinger.Ping(conn, handler.Mu); err != nil {
				logger.Log.Error("Failed to send heartbeat ping, triggering reconnect", "name", name, "err", err)
				cancel()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func MonitorConnection(
	supervisor *constants.Supervisor,
	streamCfg *constants.Stream,
	ticker *time.Ticker,
	attemptCtx context.Context,
	attemptCancel context.CancelFunc) {
	metrics.Adapter_SupervisorGoroutines.WithLabelValues(streamCfg.Name).Inc()
	defer supervisor.Wg.Done()
	defer metrics.Adapter_SupervisorGoroutines.WithLabelValues(streamCfg.Name).Dec()
	for {
		select {
		case <-ticker.C:
			if time.Since(supervisor.LastPongTime) > time.Duration(streamCfg.PongTimeout)*time.Second {
				logger.Log.Warn("Pong timeout -- cancelling the connection", "name", streamCfg.Name)
				attemptCancel()
				return
			}

		case <-attemptCtx.Done():
			return

		case <-supervisor.Ctx.Done():
			return
		}
	}
}
