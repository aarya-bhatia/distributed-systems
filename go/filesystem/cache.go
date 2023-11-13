package filesystem

import (
	log "github.com/sirupsen/logrus"
	"net"
)

// To reuse same TCP connection to download or upload multiple blocks to a replica
type ConnectionCache struct {
	conn map[string]net.Conn // maps address to connection
}

func NewConnectionCache() *ConnectionCache {
	cache := new(ConnectionCache)
	cache.conn = make(map[string]net.Conn)
	return cache
}

func (cache *ConnectionCache) GetConnection(addr string) net.Conn {
	if _, ok := cache.conn[addr]; !ok {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Warn("Failed to connect:", addr)
			return nil
		}

		// log.Info("Connected to", addr)
		cache.conn[addr] = conn
	}

	return cache.conn[addr]
}

func (cache *ConnectionCache) Close() {
	for _, conn := range cache.conn {
		// log.Debug("Closing connection with", addr)
		conn.Close()
	}
}
