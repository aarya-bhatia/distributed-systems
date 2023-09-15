package server

import (
	"fmt"
	"log"
	"net"
	"strings"
)

type RequestHandler interface {
	Handle(*Server, *net.UDPAddr, string)
}

type Server struct {
	Address    *net.UDPAddr
	Connection *net.UDPConn
}

func NewServer(port int) (*Server, error) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{Address: addr, Connection: conn}, nil
}

func (server *Server) Close() {
	server.Connection.Close()
}

func (server *Server) Listen(handler RequestHandler) {
	buffer := make([]byte, 1024)

	for {
		n, addr, err := server.Connection.ReadFromUDP(buffer)
		if err != nil {
			log.Fatal(err)
			return
		}

		message := strings.TrimSpace(string(buffer[0:n]))
		log.Printf("%s: %s\n", addr.String(), message)
		if message == "EXIT" {
			return
		}

		go handler.Handle(server, addr, message)
	}
}

