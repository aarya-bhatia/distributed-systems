package client

import (
	"cs425/filesystem/server"
	log "github.com/sirupsen/logrus"
	"net/rpc"
	"time"
)

type Heartbeat struct {
	Server   *rpc.Client
	Args     server.HeartbeatArgs
	Signal   chan bool
	Interval time.Duration
}

func NewHeartbeat(conn *rpc.Client, clientID string, resource string, interval time.Duration) *Heartbeat {
	h := new(Heartbeat)
	h.Server = conn
	h.Args = server.HeartbeatArgs{ClientID: clientID, Resource: resource}
	h.Interval = interval
	h.Signal = make(chan bool)
	return h
}

func (h *Heartbeat) Start() {
	reply := true
	for {
		select {
		case <-h.Signal:
			return
		case <-time.After(h.Interval):
			if err := h.Server.Call("Server.Heartbeat", &h.Args, &reply); err != nil {
				log.Warn(err)
			}
		}
	}
}

func (h *Heartbeat) Stop() {
	h.Signal <- true
}
