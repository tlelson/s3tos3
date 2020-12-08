package adjustableticker

import (
	"time"
)

// Ticker holds a channel that delivers `ticks` of a clock at intervals. Ticks can be adjusted after
// starting by sending on the Increment channel.  Incrementing is done exponentially.
//
// The Ticker may be used as a throttler for activating exponential back-off across concurrent
// goroutines.  If a failure is detected by one goroutine, it can send a call to the incrementing
// channel to increase the tick interval. This will suspend all ticks - aggregate them until the
// pause is over without new Increment signals - and resume ticks at a slower rate.
//
// The initial tick period is set with the Period.
//
// Provide a logger if verbose logs are required. Otherwise there will be no logging.
//
// N.B this is intended for use with highly concurrent applications.  If only a few goroutines are
// running chances are you won't need this but if you do, lower the Damp setting.  The default of 10
// means that any less that 10 Increment signals will not actually increment the ticker, simply
// pause it.
type Ticker struct {
	C chan time.Time // The channel on which the ticks are delivered.
	// Inc takes time.Now() and increments the tick interval. Sending the current time ensures that
	// globing of events from the same time frame occurs as intended.
	Inc chan time.Time

	// Configuration
	Period time.Duration  // Required. Tick interval.
	Damp   *uint          // Prevent runaway incrementing. Default 10, requires 10 events to increment.
	Pause  *time.Duration // Period to wait after last Inc before resuming ticks. Default 500ms.
	Logger Logger         // Default mode is quiet. Provide this for log output.

	// private
	stop chan bool
}

// Logger logs. Amazing huh.
type Logger interface {
	Printf(format string, v ...interface{})
}

// unLogger fulfills the logging interface but does nothing.
type unLogger struct{}

func (unLogger) Printf(string, ...interface{}) {}

// Start applies the configuration and starts the ticker
func (t *Ticker) Start() {
	// Set defaults
	if t.Damp == nil {
		var x uint = 10
		t.Damp = &x
	}
	if t.Pause == nil {
		x := 500 * time.Millisecond
		t.Pause = &x
	}
	if t.Logger == nil {
		t.Logger = unLogger{}
	}

	t.C = make(chan time.Time)
	t.Inc = make(chan time.Time)

	t.stop = t.startTimer(t.Period)
}

func (t Ticker) startTimer(d time.Duration) (quit chan bool) {
	quit = make(chan bool) // unbuffered so that sender can rely on it being read
	go func() {
		for {
			select {
			case <-quit:
				t.Logger.Printf("ticker stopped")
				return
			case <-t.Inc:
				t.Logger.Printf("Incrementing ...")
				// Drop other Inc signals until pause is achieved
				globbed := t.drain(t.Inc, *t.Pause)
				t.Logger.Printf("%d signals globed", globbed)
				if globbed >= int(*t.Damp) {
					d <<= 1
					t.Logger.Printf("Increasing tick period to: %s", d)
				}
				continue
			case t.C <- time.Now():
				// Someone wants a tick
			default:
				// pass, keep cycling looking for Inc or tick requests
			}
			time.Sleep(d)
		}
	}()

	return quit
}

// drain empties the read channel until the wait time has elapsed without receives.
func (t Ticker) drain(read chan time.Time, wait time.Duration) (globbed int) {
	last := time.Now()
	for time.Now().Before(last.Add(wait)) {
		select {
		case last = <-read:
			globbed++
		default:
			// don't block on read
		}
	}
	return
}

// Stop turns off a ticker. After Stop, no more ticks will be sent.
// Stop does not close the channel, to prevent a concurrent goroutine
// reading from the channel from seeing an erroneous "tick".
func (t Ticker) Stop() {
	t.stop <- true
	close(t.Inc)
}
