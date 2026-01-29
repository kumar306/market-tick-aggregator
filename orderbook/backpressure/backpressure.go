package backpressure

import (
	"context"
	"market-orderbook/constants"
	"shared/logger"
	"shared/metrics"
	"sync/atomic"
	"time"
)

// healthy
// throttling - usage is above threshold for K seconds

// let state be atomic as may need to reference it from consumer to pause partition fetch globally
type BPControllerState struct {
	state        atomic.Uint32
	suspectSince time.Time
}

var BPController *BPControllerState

func InitBPController() {
	BPController = &BPControllerState{}
	BPController.state.Store(uint32(constants.Healthy))
}

func RunBackpressureController(ctx context.Context, cfg *constants.BackpressureConfig, bpCh chan *constants.BackpressureEvent) {
	InitBPController()
	ticker := time.NewTicker(time.Duration(cfg.PollIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	var lastMaxUsage float64
	var minUsage float64

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done event in backpressure controller. Returning")
			return

		case ev := <-bpCh:
			lastMaxUsage = ev.MaxQueueUsage

		case <-ticker.C:
			now := time.Now()
			state := GetBackpressureState()
			switch state {
			case constants.Healthy:

				if lastMaxUsage >= cfg.QueueUsageHighThreshold {
					BPController.suspectSince = now
					BPController.state.Store(uint32(constants.Suspect))
					minUsage = lastMaxUsage
					logger.Log.Info("BP transition",
						"from", state,
						"to", constants.Suspect,
						"usage", lastMaxUsage)
					metrics.Orderbook_BackpressureState.Set(float64(constants.Suspect))
					metrics.Orderbook_BackpressureTransitionsTotal.Inc()
				}

			case constants.Suspect:

				// if maxQueueUsage dropped below low
				if lastMaxUsage < cfg.QueueUsageLowThreshold {
					BPController.suspectSince = time.Time{}
					BPController.state.Store(uint32(constants.Healthy))
					logger.Log.Info("BP transition",
						"from", state,
						"to", constants.Healthy,
						"usage", lastMaxUsage)
					metrics.Orderbook_BackpressureState.Set(float64(constants.Healthy))
					metrics.Orderbook_BackpressureTransitionsTotal.Inc()
					break
				}

				// protect against random spikes
				minUsage = min(minUsage, lastMaxUsage)

				if now.Sub(BPController.suspectSince) >= (time.Duration(cfg.ConfirmSeconds)*time.Second) &&
					minUsage >= cfg.QueueUsageHighThreshold {

					// switch to throttling
					BPController.state.Store(uint32(constants.Throttling))
					logger.Log.Info("BP transition",
						"from", state,
						"to", constants.Throttling,
						"usage", lastMaxUsage)
					logger.Log.Warn("Backpressure state made to throttling in orderbook")
					metrics.Orderbook_BackpressureState.Set(float64(constants.Throttling))
					metrics.Orderbook_BackpressureTransitionsTotal.Inc()
				}

			case constants.Throttling:

				// if usage dips below low threshold, then update back to healthy
				if lastMaxUsage < cfg.QueueUsageLowThreshold {
					BPController.suspectSince = time.Time{}
					BPController.state.Store(uint32(constants.Healthy))
					logger.Log.Info("BP transition",
						"from", state,
						"to", constants.Healthy,
						"usage", lastMaxUsage)
					logger.Log.Info("Backpressure state back to healthy")
					metrics.Orderbook_BackpressureState.Set(float64(constants.Healthy))
					metrics.Orderbook_BackpressureTransitionsTotal.Inc()
				}
			}
		}

	}
}

// get the state atomically
func GetBackpressureState() constants.BackpressureState {
	return constants.BackpressureState(int(BPController.state.Load()))
}

// k workers - k queue usages are monitored at the dispatcher level
// array of floats
// usage > threshold -> i will need to read that float t times on around >= x seconds
// at any ticker, if value < lowthreshold, abandon the ticker immediately
// else if value stays above high threshold, then change the state to throttling
// if the value fluctuates between just above high and just below high, change it to healthy at the end
// we dont want to immediately change to healthy at the moment it just goes below high threshold as this will lead to thrashing and consume CPU

// worker 1, 2 .. n
// each of these workers read from different/same partitions.. combination of them
// if even 1 worker has slowness, the entire system flow is dictated by slowest consumer
// worker 4 suddenly hit 0.61 usage. send an event to controller via channel. time elapsed since start = 0
// now the ticker will send event to same channel with time elapsed since start
// if time in the event > seconds, track the minValue of usage. init minValue = usage
// state is throttling. same state is read from dispatcher so it needs to be atomic -> 0/1/2

// if state is healthy, then only send the event to controller (this is only to start the event which changes it to throttling)
// what if state is already suspect and worker hit high queue usage? if worker already present/new worker case

// when controller receives event for a worker, he will receive the time, workerID, minUsage
// he will check in his map to see if worker already exist.
// if the time == 0, its a new event (assumed)
// if worker already in map with time > current time, then discard this update (to handle suspect case another worker comes)

// best to do global backpressure as above idea violates correctness in the system
