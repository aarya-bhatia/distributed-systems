package common

import (
	log "github.com/sirupsen/logrus"
	"net/rpc"
)

type ConnectionPool struct {
	Cluster []Node
	conn    map[int]*rpc.Client
}

func NewConnectionPool(cluster []Node) *ConnectionPool {
	pool := new(ConnectionPool)
	pool.Cluster = cluster
	pool.conn = make(map[int]*rpc.Client)
	return pool
}

func (pool *ConnectionPool) GetConnection(node int) (*rpc.Client, error) {
	if _, ok := pool.conn[node]; !ok {
		conn, err := Connect(node, pool.Cluster)
		if err != nil {
			log.Warn("Failed to connect:", node)
			return nil, err
		}
		pool.conn[node] = conn
	}
	return pool.conn[node], nil
}

func (pool *ConnectionPool) Close() {
	for key, conn := range pool.conn {
		conn.Close()
		delete(pool.conn, key)
	}
}
