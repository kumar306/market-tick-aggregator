package worker

import (
	"context"
	"market-aggregator/constants"
	"market-aggregator/internal"
	"shared/logger"
)

// make worker a struct so it has its state within instead of passing state like a parameter
// and let the worker have process and flush function
type Worker struct {
	ID          int
	Channel     chan *constants.DispatchRecord
	SymbolState map[string]*WindowState
}

type WindowState struct {
	Windows map[string]*constants.Window
}

func NewWorker(id int, ch chan *constants.DispatchRecord) *Worker {
	symbolState := make(map[string]*WindowState)
	return &Worker{
		ID:          id,
		Channel:     ch,
		SymbolState: symbolState,
	}
}

func (w *Worker) Run(ctx context.Context, idx int, ch chan *constants.DispatchRecord, cfg []*constants.WindowConfig) {
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received context shutdown. Stopping aggregator worker channel", "idx", idx)
			return
		case dispatchRec, ok := <-ch:
			if !ok {
				logger.Log.Error("The worker channel is closed", "workerIdx", idx)
				return
			}
			switch dispatchRec.Event {
			case constants.ProcessEvent:
				w.ProcessTick(ctx, cfg, dispatchRec)
			case constants.FlushEvent:
				w.FlushWindows()
			default:
				logger.Log.Info("Aggregator worker event received didn't match any known event", "event", dispatchRec.Event)
			}
		}
	}
}

func (w *Worker) ProcessTick(ctx context.Context,
	cfg []*constants.WindowConfig,
	dispatchRec *constants.DispatchRecord) {
	// if not present, wire it and create all metrics - from the wired registry
	// else skip
	// update all window metrics

	workerState := w.SymbolState
	_, ok := workerState[dispatchRec.BufferKey]
	if !ok {
		// wire up the state
		// create window objects. for each window object, wire up the metrics
		// get window objects created via a window registry
		windowState := &WindowState{
			Windows: internal.BuildWindows(cfg),
		}

		w.SymbolState[dispatchRec.BufferKey] = windowState
	}

	tick := dispatchRec.Tick

	for _, window := range w.SymbolState[dispatchRec.BufferKey].Windows {
		for _, metric := range window.Metrics {
			metric.Update(tick)
		}
	}
}

func (w *Worker) FlushWindows() {
	// flushing for a particular window ID
	// call snapshot
	// post into kafka aggregated_ticks
}
