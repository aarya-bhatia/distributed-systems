package maplejuice

import (
	"bufio"
	"cs425/common"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
)

func (server *Leader) listDirectory(name string) []string {
	res := []string{}
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", server.Info.Hostname, server.Info.FrontendPort))
	if err != nil {
		return res
	}
	defer conn.Close()
	if !common.SendMessage(conn, "lsdir "+name) {
		return res
	}

	reader := bufio.NewReader(conn)

	line, err := reader.ReadString('\n')
	if err != nil {
		return res
	}
	if line[:len(line)-1] != "OK" {
		log.Println(line[:len(line)-1])
		return res
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		res = append(res, line[:len(line)-1])
	}

	log.Println("lsdir "+name+":", res)
	return res
}
