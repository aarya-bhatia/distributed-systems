package common

import (
	log "github.com/sirupsen/logrus"
	"net/rpc"
)

type ConnectionPool struct {
	conn map[int]*rpc.Client
}

func NewConnectionPool() *ConnectionPool {
	pool := new(ConnectionPool)
	pool.conn = make(map[int]*rpc.Client)
	return pool
}

func (pool *ConnectionPool) GetConnection(node int) (*rpc.Client, error) {
	if _, ok := pool.conn[node]; !ok {
		conn, err := Connect(node)
		if err != nil {
			log.Warn("Failed to connect:", node)
			return nil, err
		}
		pool.conn[node] = conn
	}
	return pool.conn[node], nil
}

func (pool *ConnectionPool) Close() {
	for _, conn := range pool.conn {
		conn.Close()
	}
}
