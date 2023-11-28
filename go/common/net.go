package common

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
)

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

func Connect(nodeID int, cluster []Node) (*rpc.Client, error) {
	node := GetNodeByID(nodeID, cluster)
	if node == nil {
		return nil, errors.New("Unknown Node")
	}
	addr := GetAddress(node.Hostname, node.RPCPort)
	return rpc.Dial("tcp", addr)
}
