package common

import (
	log "github.com/sirupsen/logrus"
	"net/rpc"
)

type ConnectionPool struct {
	nodeType int
	conn    map[int]*rpc.Client
}

func NewConnectionPool(nodeType int) *ConnectionPool {
	pool := new(ConnectionPool)
	pool.nodeType = nodeType
	pool.conn = make(map[int]*rpc.Client)
	return pool
}

func (pool *ConnectionPool) GetConnection(node int) (*rpc.Client, error) {
	if _, ok := pool.conn[node]; !ok {
		conn, err := Connect(node, pool.nodeType)
		if err != nil {
			log.Warn("Failed to connect:", node)
			return nil, err
		}
		pool.conn[node] = conn
	}
	return pool.conn[node], nil
}

func (pool *ConnectionPool) RemoveConnection(node int) {
	if conn, ok := pool.conn[node]; ok {
		conn.Close()
		delete(pool.conn, node)
	}
}

func (pool *ConnectionPool) Close() {
	for key, conn := range pool.conn {
		conn.Close()
		delete(pool.conn, key)
	}
}
