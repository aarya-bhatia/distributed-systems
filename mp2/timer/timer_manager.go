package timer

import (
	"time"
)

const T_GOSSIP = 5 * time.Second   // Time duration between each gossip round
const T_TIMEOUT = 10 * time.Second // Time duration until a peer times out
var T_CLEANUP = 5 * time.Second    // Time duration before peer is deleted

type TimerManager struct {
	Timers         map[string]*Timer
	TimeoutChannel chan TimerEvent
}

func NewTimerManager() *TimerManager {
	tm := &TimerManager{}
	tm.Timers = make(map[string]*Timer)
	tm.TimeoutChannel = make(chan TimerEvent)
	return tm
}

func (tm *TimerManager) StartTimer(id string, duration time.Duration) {
	tm.Timers[id] = &Timer{ID: id, TimerChannel: make(chan TimerEvent, 1), TimeoutChannel: tm.TimeoutChannel, TimeoutDuration: duration, Alive: true}
	go tm.Timers[id].Start()
}

func (tm *TimerManager) StopTimer(id string) {
	if timer, ok := tm.Timers[id]; ok {
		timer.Stop()
		delete(tm.Timers, id)
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
