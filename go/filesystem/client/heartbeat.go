package client

import (
	"cs425/filesystem/server"
	log "github.com/sirupsen/logrus"
	"net/rpc"
	"time"
)

type Heartbeat struct {
	ClientID string
	Server   *rpc.Client
	Signal   chan bool
	Interval time.Duration
}

func NewHeartbeat(conn *rpc.Client, clientID string, interval time.Duration) *Heartbeat {
	h := new(Heartbeat)
	h.Server = conn
	h.ClientID = clientID
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
			if err := h.Server.Call(server.RPC_HEARTBEAT, &h.ClientID, &reply); err != nil {
				log.Warn(err)
			}
		}
	}
}

func (h *Heartbeat) Stop() {
	h.Signal <- true
}
