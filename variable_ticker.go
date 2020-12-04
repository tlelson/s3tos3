package s3tos3

import (
	"time"
)

// adjustableTicker holds a channel that delivers `ticks' of a clock at intervals. Ticks can be adjusted
// after starting by sending on the Increment channel.  Incrimenting is done exponentially.  Should
// be instatiated with NewAdjustableTicker.
type adjustableTicker struct {
	C      chan time.Time // The channel on which the ticks are delivered.
	Inc    chan bool      // Incriment the tick interval
	Logger interface {
		Printf(format string, v ...interface{})
	}

	// private
	stop chan bool
}

// TODO: make own package. Rename to New
// scheduler returns an Adjustable Ticker that may be used as a throttler implimenting Exponential
// backoff.  If a failure is detected, the detector can send a call to the incrimenting channel to
// increase the tick interval
//
// The initial tick period is set with the input variable d. When incGlobb is non-zero, all requests
// to incriment the tick interval within that period are globbed to prevent radical runnaway of the
// the tick interval.
//
// E.g: Suppose 10 go routines all timeout in the same 1 second period and they all send a call to
// incriment the ticker from the same timeout event. Using incGlob of 1 second means that the first
// is received and all other sends to incriment the ticker in the next second are discarded.
func scheduler(d, incGlob time.Duration, preload uint, l logger) *adjustableTicker {
	ticks := make(chan time.Time, preload)
	inc := make(chan bool)

	at := &adjustableTicker{C: ticks, Inc: inc, Logger: l}
	at.stop = at.startTimer(d, incGlob)
	return at
}

func (t *adjustableTicker) startTimer(d, incGlob time.Duration) (quit chan bool) {
	quit = make(chan bool) // unbuffered so that sender can rely on it being read
	go func() {
		for {
			select {
			case <-quit:
				return
			case <-t.Inc:
				// No more ticks get sent until the current failures are dropped
				stop := t.drain(t.Inc)
				time.Sleep(incGlob)
				stop <- true // blocks until drain complete
				d <<= 1
				t.Logger.Printf("Increasing tick period to: %s", d)
			default:
				// pass
			}
			time.Sleep(d)
			t.C <- time.Now()
		}
	}()
	return quit
}

func (t adjustableTicker) drain(read chan bool) (quit chan bool) {
	quit = make(chan bool)
	go func() {
		var globbed int
		for {
			select {
			case <-quit:
				t.Logger.Printf("%d signals globbed", globbed)
				return
			case <-read:
				globbed++
				// dump the value
			}
		}
	}()
	return quit
}

// Stop turns off a ticker. After Stop, no more ticks will be sent.
// Stop does not close the channel, to prevent a concurrent goroutine
// reading from the channel from seeing an erroneous "tick".
func (t *adjustableTicker) Stop() {
	t.stop <- true
	close(t.C)
	close(t.Inc)
}
