package internal

import (
	"context"
	"market-adapter/constants"
	"sync"
)

// starts the supervisor per stream. each stream has a supervisor to manage everything
func StartSupervisor(supervisor *constants.Supervisor,
	feed *constants.Feed,
	stream *constants.Stream,
	parentCtx context.Context,
	wg *sync.WaitGroup) {

	// keep trying to acquire the connection - until max retries
	// each feed has its configured retry limit and backoff time

	// pass into spawned goroutines to handle shutdown
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	supervisor.Ctx = ctx
	supervisor.Cancel = cancel
	supervisor.StatusChan <- constants.StatusNew

	wg.Add(1)
	go SupervisorLoop(feed, stream, supervisor, wg)
	wg.Wait()
}
