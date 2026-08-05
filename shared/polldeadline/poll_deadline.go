package polldeadline

import (
	"context"
	"time"
)

// A substitute for timed context. After kgo's pollFetches, instead of ctx.withTimeout, use a poll deadline for the worker to reset the timer instead of allocating new ctx
// using a custom poll deadline instead of context.withTimeout which allocated 200 bytes per message on heap on hot path
// now no heap allocations with PollDeadline
// single goroutine owns, so its safe. go suggests not to have multiple goroutines modifying a timer
type PollDeadline struct {
	timer  *time.Timer
	armReq chan time.Duration
	done   chan struct{} // size of 1, reused forever - signaled by send, never closed
}

func New() *PollDeadline {
	d := &PollDeadline{
		timer:  time.NewTimer(time.Hour),
		armReq: make(chan time.Duration),
		done:   make(chan struct{}, 1),
	}
	// create an empty timer object on poll deadline
	d.timer.Stop()
	go d.loop()
	return d
}

func (d *PollDeadline) loop() {
	for {
		// worker restarts the timer after the poll fetches is done, for the next message
		timeout := <-d.armReq
		d.timer.Reset(timeout)

		for {
			select {
			// either timer expired first naturally before the timeout of pollfetches was reached
			// emit to done and rearm the timer without change
			case <-d.timer.C:
				select {
				case d.done <- struct{}{}:
				default:
				}
				goto next

			// pollFetches returned early as we got the data. stop stale timer and restart with new timeout
			case timeout = <-d.armReq:

				// if at the same time the timer fired naturally, its a ghost value so empty the channel
				// code inside !d.timer.Stop()
				if !d.timer.Stop() {
					select {
					case <-d.timer.C:
					default:
					}
				}
				d.timer.Reset(timeout)
			}
		}
	next:
	}
}

// rearms the deadline for the next cycle and returns a context.Context
// bound to it. call immediately before each use (e.g. each PollFetches call).
func (d *PollDeadline) Reset(timeout time.Duration) context.Context {
	// drain any stale signal left over from a cycle that ended early.
	select {
	case <-d.done:
	default:
	}
	d.armReq <- timeout
	return d
}

func (d *PollDeadline) Deadline() (time.Time, bool) { return time.Time{}, false }
func (d *PollDeadline) Done() <-chan struct{}       { return d.done }
func (d *PollDeadline) Value(key any) any           { return nil }
func (d *PollDeadline) Err() error {
	select {
	case <-d.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}
