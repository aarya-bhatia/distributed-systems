package timer

import (
	"sync"
	"time"
)

type TimerManager struct {
	Timers         map[string]*Timer
	TimeoutChannel chan TimerEvent
	Mutex          sync.Mutex
}

func NewTimerManager() *TimerManager {
	tm := &TimerManager{}
	tm.Timers = make(map[string]*Timer)
	tm.TimeoutChannel = make(chan TimerEvent)
	return tm
}

func (tm *TimerManager) StartTimer(id string, duration time.Duration) {
	tm.Mutex.Lock()
	defer tm.Mutex.Unlock()
	tm.Timers[id] = &Timer{ID: id, TimerChannel: make(chan TimerEvent, 1), TimeoutChannel: tm.TimeoutChannel, TimeoutDuration: duration, Alive: true}
	go tm.Timers[id].Start()
}

func (tm *TimerManager) StopTimer(id string) {
	tm.Mutex.Lock()
	defer tm.Mutex.Unlock()
	if timer, ok := tm.Timers[id]; ok {
		timer.Stop()
		delete(tm.Timers, id)
	}
}

func (tm *TimerManager) StopAll() {
	for ID := range tm.Timers {
		tm.StopTimer(ID)
	}
}

func (tm *TimerManager) RestartTimer(id string, duration time.Duration) {
	tm.StopTimer(id)
	tm.StartTimer(id, duration)
}

func (tm *TimerManager) Close() {
	for _, t := range tm.Timers {
		t.Stop()
	}

	close(tm.TimeoutChannel)
}
