package dispatcher

import (
	"market-aggregator/constants"
)

// used only for flush-event fan-out by flush schedulers.
// ticks no longer flow through a shared dispatcher.
// each worker consumes its own assigned partitions directly via its own GroupTransactSession.
func CreateWorkerChannels(workerCount int, chanSize int) []chan *constants.DispatchRecord {
	var workerChannels []chan *constants.DispatchRecord

	for i := 0; i < workerCount; i++ {
		ch := make(chan *constants.DispatchRecord, chanSize)
		workerChannels = append(workerChannels, ch)
	}

	return workerChannels
}
