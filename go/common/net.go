package common

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
)

const MAPLEJUICE_NODE = 0
const SDFS_NODE = 1

func GetAddress(hostname string, port int) string {
	return fmt.Sprintf("%s:%d", hostname, port)
}

func StartRPCServer(Hostname string, Port int, Service interface{}) {
	listener, err := net.Listen("tcp4", GetAddress(Hostname, Port))
	if err != nil {
		log.Fatal("Error starting server: ", err)
	}

	if err := rpc.Register(Service); err != nil {
		log.Fatal(err)
	}

	log.Println("Running PRC server at", GetAddress(Hostname, Port))

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go rpc.ServeConn(conn)
	}
}

func Connect(nodeID int, nodeType int) (*rpc.Client, error) {
	node := GetNodeByID(nodeID)
	if node == nil {
		return nil, errors.New("Unknown node ID")
	}

	var addr string

	if nodeType == MAPLEJUICE_NODE {
		addr = GetAddress(node.Hostname, node.MapleJuiceRPCPort)
	} else if nodeType == SDFS_NODE {
		addr = GetAddress(node.Hostname, node.SDFSRPCPort)
	} else {
		return nil, errors.New("Unknown node type")
	}

	return rpc.Dial("tcp", addr)
}
