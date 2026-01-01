package worker

import (
	"context"
	"market-aggregator/constants"
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

func (w *Worker) Run(ctx context.Context, idx int, ch chan *constants.DispatchRecord) {
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
				w.ProcessTick(ctx, dispatchRec)
			case constants.FlushEvent:
				w.FlushWindows()
			default:
				logger.Log.Info("Aggregator worker event received didn't match any known event", "event", dispatchRec.Event)
			}
		}
	}
}

func (w *Worker) ProcessTick(ctx context.Context,
	dispatchRec *constants.DispatchRecord) {

}

func (w *Worker) FlushWindows() {

}
