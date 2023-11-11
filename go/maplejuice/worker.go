package maplejuice

import (
	"cs425/common"
	log "github.com/sirupsen/logrus"
	"net"
)

func (worker *Worker) handleConnection(conn net.Conn) {
	defer conn.Close()
}

func (worker *Worker) Start(hostname string, port int) {
	listener, err := net.Listen("tcp", common.GetAddress(hostname, port))
	if err != nil {
		log.Fatal(err)
	}

	log.Info("MapleJuice worker is running at ", common.GetAddress(hostname, port))

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go worker.handleConnection(conn)
	}
}
