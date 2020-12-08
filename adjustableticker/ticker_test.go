package adjustableticker_test

import (
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tlelson/s3tos3/adjustableticker"
)

type logger struct{}

func (logger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

var l = logger{}

func Test_Basic(t *testing.T) {
	at := adjustableticker.Ticker{
		Period: 200 * time.Millisecond,
		//Logger: l,
	}
	at.Start()
	defer at.Stop()

	begin := time.Now()
	for i := 0; i < 4; i++ {
		<-at.C
	}
	d := time.Since(begin)

	if d > 805*time.Millisecond {
		t.Errorf("Expected to complete in less than 1s. Took %s", d)
	}
}

func Test_IncrimentSimple(t *testing.T) {
	var damp uint = 0 // Ensure every Increment signal counts
	at := adjustableticker.Ticker{
		Period: 200 * time.Millisecond,
		Damp:   &damp,
		//Logger: l,
	}
	at.Start()
	defer at.Stop()

	<-at.C
	at.Inc <- time.Now() // should pause for default 500ms
	t2 := <-at.C
	t3 := <-at.C

	if d := time.Duration(t3.Sub(t2)).Round(time.Millisecond); d != 400*time.Millisecond {
		t.Errorf("Tick after increment should double 200ms. Actual interval %s", d)
	}

}

func Test_IncrimentGlobbing(t *testing.T) {
	var damp uint = 10
	at := adjustableticker.Ticker{
		Period: 200 * time.Millisecond,
		//Logger: l,
		Damp: &damp,
	}
	at.Start()
	defer at.Stop()

	quitJobs := make(chan bool)
	jobs := func() {
		for {
			select {
			case <-at.C:
				// do work
			case <-quitJobs:
				return
			}
		}
	}
	go jobs()

	time.Sleep(100 * time.Millisecond) // Wait for first tick/job to run

	// Shoot 5 Inc signals for 1 second ... Should be globed
	for start := time.Now(); time.Now().Before(start.Add(time.Second)); {
		at.Inc <- time.Now()
		time.Sleep(20 * time.Millisecond)
	}

	// Stop pulling ticks
	quitJobs <- true

	t1 := <-at.C
	t2 := <-at.C

	if d := time.Duration(t2.Sub(t1)).Round(time.Millisecond); d != 400*time.Millisecond {
		t.Errorf("Tick interval after globed incrementation should be double 200ms. Actual interval %s", d)
	}
}

func Test_NoTickWhenIncrimenting(t *testing.T) {
	at := adjustableticker.Ticker{
		Period: 200 * time.Millisecond,
		//Logger: l,
	}
	at.Start()
	defer at.Stop()

	var tickCount int32

	quitJobs := make(chan bool)
	jobs := func() {
		for {
			select {
			case <-at.C:
				atomic.AddInt32(&tickCount, 1)
			case <-quitJobs:
				return
			}
		}
	}

	// Start jobs and give it a while to tick
	go jobs()
	time.Sleep(250 * time.Millisecond)
	at.Inc <- time.Now()
	ticksBefore := atomic.LoadInt32(&tickCount)
	// Stop ticks for 1s - glob 9 others
	for i := 0; i < 9; i++ {
		at.Inc <- time.Now()
		time.Sleep(20 * time.Millisecond)
	}
	quitJobs <- true
	ticksAfter := atomic.LoadInt32(&tickCount)

	// Total time running should be about 1050 Milliseconds, which would be around 5 ticks if not
	// paused.
	if ticksAfter-ticksBefore != 0 {
		t.Errorf("Expected 0 ticks after Incrimenting, got %d", tickCount)
	}

}
