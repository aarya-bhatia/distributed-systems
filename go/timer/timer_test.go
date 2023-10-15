package timer

import (
	"testing"
	"time"
)

func Test1(t *testing.T) {
	timeout := 5 * time.Second
	tm := NewTimerManager()
	ID := "test"
	tm.StartTimer(ID, timeout)
	time.Sleep(2 * time.Second)
	tm.RestartTimer(ID, timeout)
	<-tm.TimeoutChannel
	time.Sleep(2 * time.Second)
}
