package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

type Host struct {
	Address   string // IP address
	Port      int    // server port
	ID        string // unique ID
	Counter   int    // heartbeat counter
	UpdatedAt int64  // local timestamp when counter last updated
	Suspected bool   // whether the node is suspected of failure
}

type Server struct {
	HostName   string
	ID         string
	Address    *net.UDPAddr
	Connection *net.UDPConn
	Members    map[string]*Host
	MemberLock sync.Mutex
	Introducer bool
}

func NewHost(Address string, Port int, ID string) *Host {
	var host = &Host{}
	host.Address = Address
	host.Port = Port
	host.ID = ID
	host.Counter = 0
	host.UpdatedAt = 0
	return host
}

func (host Host) GetSignature() string {
	return fmt.Sprintf("%s:%d:%s", host.Address, host.Port, host.ID)
}

func (host Host) GetSignatureWithCount() string {
	return fmt.Sprintf("%s:%d:%d", host.GetSignature(), host.Counter, host.UpdatedAt)
}

func NewServer(hostname string, port int, id string) (*Server, error) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	return &Server{ID: id, HostName: hostname, Address: addr, Connection: conn, Members: make(map[string]*Host), Introducer: false}, nil
}

func (server *Server) AddHost(address string, port int, id string) (*Host, error) {
	server.MemberLock.Lock()
	defer server.MemberLock.Unlock()

	if found, ok := server.Members[id]; ok {
		log.Println(found)
		return nil, errors.New("A peer with this ID already exists")
	}

	if server.Introducer {
		for _, member := range server.Members {
			if member.Address == address && member.Port == port {
				prevID := member.ID
				member.ID = id
				server.Members[id] = member
				delete(server.Members, prevID)
				return member, nil
			}
		}
	}

	server.Members[id] = NewHost(address, port, id)
	return server.Members[id], nil
}

func (server *Server) GetPacket() (message string, addr *net.UDPAddr, err error) {
	buffer := make([]byte, 1024)

	n, addr, err := server.Connection.ReadFromUDP(buffer)
	if err != nil {
		return "", nil, err
	}
	message = strings.TrimSpace(string(buffer[0:n]))
	log.Printf("Received %d bytes from %s\n", len(message), addr.String())
	return message, addr, nil
}

func (server *Server) SendPacket(address string, port int, data []byte) error {
	client, err := net.Dial("udp", fmt.Sprintf("%s:%d", address, port))
	if err != nil {
		return err
	}

	defer client.Close()
	_, err = client.Write(data)
	return err
}

func (server *Server) Close() {
	server.Connection.Close()
}

func (server *Server) EncodeMembersList() string {
	var arr = []string{}

	server.MemberLock.Lock()
	defer server.MemberLock.Unlock()

	for _, host := range server.Members {
		arr = append(arr, host.GetSignatureWithCount())
	}

	return strings.Join(arr, ";")
}

func (s *Server) GetJoinMessage() string {
	return fmt.Sprintf("JOIN %s:%d:%s\n", s.HostName, s.Address.Port, s.ID)
}

func (s *Server) GetLeaveMessage() string {
	return fmt.Sprintf("LEAVE %s:%d:%s\n", s.HostName, s.Address.Port, s.ID)
}

func (s *Server) GetPingMessage() string {
	return fmt.Sprintf("PING %s:%d:%s\n%s\n", s.HostName, s.Address.Port, s.ID, s.EncodeMembersList())
}
