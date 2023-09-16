package timer

import (
	"fmt"
	"testing"
	"time"
)

func TestTimer(t *testing.T) {
	tm := NewTimerManager(10 * time.Second)
	tm.RestartTimer("one")
	time.Sleep(5 * time.Second)
	tm.RestartTimer("one")
	event := <-tm.TimeoutChannel
	fmt.Println(event)
	tm.StopAll()
	// tm.RestartTimer("one")
	// tm.StopAll()
	time.Sleep(2 * time.Second)
}

