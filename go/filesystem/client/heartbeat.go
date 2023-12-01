package client

import (
	"cs425/common"
	"cs425/filesystem/server"
	"net/rpc"
	"time"

	log "github.com/sirupsen/logrus"
)

type Heartbeat struct {
	ClientID string
	Server   *rpc.Client
	Signal   chan bool
	Interval time.Duration
}

func NewHeartbeat(leader int, clientID string, interval time.Duration) (*Heartbeat, error) {
	h := new(Heartbeat)
	conn, err := common.Connect(leader, common.SDFS_NODE)
	if err != nil {
		return nil, err
	}
	h.Server = conn
	h.ClientID = clientID
	h.Interval = interval
	h.Signal = make(chan bool)
	return h, nil
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
	h.Server.Close()
}
