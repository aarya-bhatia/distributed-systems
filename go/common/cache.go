package common

import (
	log "github.com/sirupsen/logrus"
	"net/rpc"
)

type ConnectionPool struct {
	conn map[string]*rpc.Client
}

func NewConnectionCache() *ConnectionPool {
	cache := new(ConnectionPool)
	cache.conn = make(map[string]*rpc.Client)
	return cache
}

func (cache *ConnectionPool) GetConnection(addr string) *rpc.Client {
	if _, ok := cache.conn[addr]; !ok {
		conn, err := rpc.Dial("tcp", addr)
		if err != nil {
			log.Warn("Failed to connect:", addr)
			return nil
		}

		cache.conn[addr] = conn
	}

	return cache.conn[addr]
}

func (cache *ConnectionPool) Close() {
	for _, conn := range cache.conn {
		conn.Close()
	}
}
