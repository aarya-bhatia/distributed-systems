package maplejuice

import (
	"cs425/common"
	"net"

	log "github.com/sirupsen/logrus"
)

type Worker struct {
	Hostname string
	Port     int
}

func (worker *Worker) handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, common.MIN_BUFFER_SIZE)
	n, err := conn.Read(buffer)
	if err != nil {
		return
	}
	log.Println("Received:", string(buffer[:n-1]))
	if string(buffer[:n-1]) == "TEST" {
		common.SendMessage(conn, "OK")
	}
}

func (worker *Worker) Start() {
	addr := common.GetAddress(worker.Hostname, worker.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	log.Info("MapleJuice worker is running at ", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go worker.handleConnection(conn)
	}
}
