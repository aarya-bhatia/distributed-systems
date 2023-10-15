package timer

import (
	"cs425/common"
	// "fmt"
	"sync"
	"time"
)

var Log = common.Log

const (
	TIMER_TIMEOUT_EVENT = 1
	TIMER_STOP_EVENT    = 2
)

type TimerEvent struct {
	EventType int
	ID        string
}

type Timer struct {
	ID              string
	TimerChannel    chan TimerEvent
	TimeoutChannel  chan TimerEvent
	TimeoutDuration time.Duration
	Alive           bool
	Mutex           sync.Mutex
}

func (timer *Timer) Start() {
	// Log.Debug(fmt.Sprintf("Timer %s has started.\n", timer.ID))
	select {
	case event := <-timer.TimerChannel:
		if event.EventType == TIMER_STOP_EVENT {
			// Log.Debugf("Timer %s has stopped.\n", timer.ID)
			timer.Mutex.Lock()
			timer.Alive = false
			close(timer.TimerChannel)
			timer.Mutex.Unlock()
		}
	case <-time.After(timer.TimeoutDuration):
		// Log.Debugf("Timer %s has timed out.\n", timer.ID)
		timer.Mutex.Lock()
		if !timer.Alive {
			close(timer.TimerChannel)
			timer.Mutex.Unlock()
			return
		}
		timer.TimeoutChannel <- TimerEvent{EventType: TIMER_TIMEOUT_EVENT, ID: timer.ID}
		timer.Alive = false
		close(timer.TimerChannel)
		timer.Mutex.Unlock()
	}
}

func (timer *Timer) Stop() {
	timer.Mutex.Lock()
	if timer.Alive {
		select {
		case timer.TimerChannel <- TimerEvent{EventType: TIMER_STOP_EVENT, ID: timer.ID}:
		default: // channel is closed
		}
	}
	timer.Mutex.Unlock()
}
