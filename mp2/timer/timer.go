package timer

import (
	"sync"
	"time"
	"log"
)

const (
	TIMER_RESTART_EVENT = 0
	TIMER_TIMEOUT_EVENT = 1
	TIMER_STOP_EVENT    = 2
)

type Timer struct {
	ID              string
	TimerChannel    chan TimerEvent
	TimeoutChannel  chan TimerEvent
	TimeoutDuration time.Duration
	Alive           bool
	Mutex           sync.Mutex
}

type TimerEvent struct {
	EventType int
	ID        string
}

type TimerManager struct {
	Timers          map[string]*Timer
	TimeoutChannel  chan TimerEvent
	TimeoutDuration time.Duration
}

func NewTimer(id string, timerChannel chan TimerEvent, timeoutChannel chan TimerEvent, timeoutDuration time.Duration) *Timer {
	t := &Timer{}
	t.ID = id
	t.TimerChannel = timerChannel
	t.TimeoutChannel = timeoutChannel
	t.Alive = false
	t.TimeoutDuration = timeoutDuration
	return t
}

func NewTimerManager(timeoutDuration time.Duration) *TimerManager {
	tm := &TimerManager{}
	tm.Timers = make(map[string]*Timer)
	tm.TimeoutDuration = timeoutDuration
	tm.TimeoutChannel = make(chan TimerEvent)
	return tm
}

func (tm *TimerManager) StopAll() {
	for id := range tm.Timers {
		tm.StopTimer(id)
	}
}

func (timer *Timer) Start() {
	timer.Mutex.Lock()
	timer.Alive = true
	timer.Mutex.Unlock()

	log.Printf("Timer %s has started.\n", timer.ID)

	for {
		select {
		case event := <-timer.TimerChannel:
			if event.EventType == TIMER_RESTART_EVENT && event.ID == timer.ID {
				log.Printf("Timer %s is restarting.\n", timer.ID)
				continue
			} else if event.EventType == TIMER_STOP_EVENT && event.ID == timer.ID {
				log.Printf("Timer %s has stopped.\n", timer.ID)
				timer.Mutex.Lock()
				timer.Alive = false
				timer.Mutex.Unlock()
				return
			}
		case <-time.After(timer.TimeoutDuration):
			log.Printf("Timer %s has timed out.\n", timer.ID)
			timer.TimeoutChannel <- TimerEvent{EventType: TIMER_TIMEOUT_EVENT, ID: timer.ID}
		}
	}
}

func (tm *TimerManager) RestartTimer(id string) {
	_, present := tm.Timers[id]
	if !present {
		timerChannel := make(chan TimerEvent)
		tm.Timers[id] = NewTimer(id, timerChannel, tm.TimeoutChannel, tm.TimeoutDuration)
		go tm.Timers[id].Start()
		return
	}

	timer := tm.Timers[id]
	timer.Mutex.Lock()
	alive := timer.Alive
	timer.Mutex.Unlock()
	if !alive {
		go tm.Timers[id].Start()
		return
	}

	timer.TimerChannel <- TimerEvent{EventType: TIMER_RESTART_EVENT, ID: id}
}

func (tm *TimerManager) StopTimer(id string) {
	timer, present := tm.Timers[id]
	if !present {
		return
	}

	timer.Mutex.Lock()
	alive := timer.Alive
	timer.Mutex.Unlock()
	if !alive {
		return
	}

	timer.TimerChannel <- TimerEvent{EventType: TIMER_STOP_EVENT, ID: id}
}
