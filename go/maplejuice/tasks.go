package maplejuice

import (
	"cs425/common"
	"fmt"
	"net"
	"time"
)

type TaskData interface {
}

type Task interface {
	Start(worker string, data TaskData) bool
	Restart(worker string) bool
}

type MapTask struct {
	Filename    string
	OffsetLines int
	CountLines  int
}

func (task MapTask) Start(worker string, data TaskData) bool {
	// lines := data.([]string)
	conn, err := net.Dial("tcp", worker)
	if err != nil {
		return false
	}
	request := fmt.Sprintf("TEST")
	if !common.SendMessage(conn, request) {
		return false
	}
	time.Sleep(1 * time.Second)
	if !common.CheckMessage(conn, "OK") {
		return false
	}
	return true
}

func (task MapTask) Restart(worker string) bool {
	time.Sleep(1 * time.Second)
	return true
}
