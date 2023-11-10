package worker

import (
	"bufio"
	"cs425/common"
	"fmt"
	"net"
	"strings"

	log "github.com/sirupsen/logrus"
)

func Start(hostname string, port int) {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", hostname, port))
	if err != nil {
		log.Fatal("Error starting server: ", err)
	}

	log.Infof("MapleJuice worker is running at %s:%d...\n", hostname, port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		buffer, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		buffer = buffer[:len(buffer)-1]
		tokens := strings.Split(buffer, " ")

		log.Printf("Request from %s: %s\n", conn.RemoteAddr(), buffer)

		if tokens[0] == "TEST" {
			common.SendMessage(conn, "OK")
			return
		}

		common.SendMessage(conn, "ERROR Malformed Request")
		return
	}
}
