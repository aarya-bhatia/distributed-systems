package timer

import (
	"log"
	"time"
)

const T_GOSSIP = 5 * time.Second   // Time duration between each gossip round
const T_TIMEOUT = 10 * time.Second // Time duration until a peer times out
var T_CLEANUP = 5 * time.Second    // Time duration before peer is deleted

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
}

type TimerManager struct {
	Timers          map[string]*Timer
	TimeoutChannel  chan TimerEvent
}

func NewTimerManager() *TimerManager {
	tm := &TimerManager{}
	tm.Timers = make(map[string]*Timer)
	tm.TimeoutChannel = make(chan TimerEvent)
	return tm
}

func (tm *TimerManager) StopAll() {
	for id := range tm.Timers {
		tm.StopTimer(id)
	}
}

func (timer *Timer) Start() {
	log.Printf("Timer %s has started.\n", timer.ID)
	select {
	case event := <-timer.TimerChannel:
		if event.EventType == TIMER_STOP_EVENT {
			log.Printf("Timer %s has stopped.\n", timer.ID)
			return
		}
	case <-time.After(timer.TimeoutDuration):
		log.Printf("Timer %s has timed out.\n", timer.ID)
		timer.TimeoutChannel <- TimerEvent{EventType: TIMER_TIMEOUT_EVENT, ID: timer.ID}
	}
}

func (timer *Timer) Stop() {
	timer.TimerChannel <- TimerEvent{EventType: TIMER_STOP_EVENT, ID: timer.ID}
}

func (tm *TimerManager) RestartTimer(id string, duration time.Duration) {
	tm.StopTimer(id)
	tm.StartTimer(id, duration)
}

func (tm *TimerManager) StartTimer(id string, duration time.Duration) {
	if _, ok := tm.Timers[id]; !ok {
		tm.Timers[id] = &Timer{ID: id, TimerChannel: make(chan TimerEvent), TimeoutChannel: tm.TimeoutChannel, TimeoutDuration: duration}
		go tm.Timers[id].Start()
	}
}

func (tm *TimerManager) StopTimer(id string) {
	if timer, ok := tm.Timers[id]; ok {
		timer.Stop()
		delete(tm.Timers, id)
	}
}

