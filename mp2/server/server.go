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
	Address   *net.UDPAddr
	Signature string
	ID        string
	Counter   int   // heartbeat counter
	UpdatedAt int64 // local timestamp when counter last updated
	Suspected bool  // whether the node is suspected of failure
}

type Server struct {
	ID         string
	Address    *net.UDPAddr
	Connection *net.UDPConn
	Members    map[string]*Host
	MemberLock sync.Mutex
	Introducer bool
	Signature  string
}

func NewHost(Hostname string, Port int, ID string, Address *net.UDPAddr) *Host {
	var host = &Host{}
	host.ID = ID
	host.Address = Address
	host.Signature = fmt.Sprintf("%s:%d:%s", Hostname, Port, ID)
	host.Counter = 0
	host.UpdatedAt = 0
	return host
}

func NewServer(Hostname string, Port int, ID string) (*Server, error) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", Port))
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	return &Server{ID: ID, Address: addr, Connection: conn, Members: make(map[string]*Host), Introducer: false, Signature: fmt.Sprintf("%s:%d:%s", Hostname, Port, ID)}, nil
}

func (server *Server) AddHost(Hostname string, Port int, ID string) (*Host, error) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", Hostname, Port))
	if err != nil {
		return nil, err
	}

	server.MemberLock.Lock()
	defer server.MemberLock.Unlock()

	if found, ok := server.Members[ID]; ok {
		log.Println(found)
		return nil, errors.New("A peer with this ID already exists")
	}

	signPre := fmt.Sprintf("%s:%d", Hostname, Port)

	if server.Introducer {
		for _, member := range server.Members {
			if strings.Index(member.Signature, signPre) == 0 {
				prevID := member.ID
				member.ID = ID
				server.Members[ID] = member
				delete(server.Members, prevID)
				return member, nil
			}
		}
	}

	server.Members[ID] = NewHost(Hostname, Port, ID, addr)
	log.Printf("Added new host: %s\n", server.Members[ID].Signature)
	return server.Members[ID], nil
}

func (server *Server) GetPacket() (message string, addr *net.UDPAddr, err error) {
	buffer := make([]byte, 1024)
	n, addr, err := server.Connection.ReadFromUDP(buffer)
	if err != nil {
		return "", nil, err
	}
	message = strings.TrimSpace(string(buffer[:n]))
	log.Printf("Received %d bytes from %s\n", len(message), addr.String())
	return message, addr, nil
}

func (server *Server) SendPacket(address string, port int, data []byte) (int, error) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", address, port))
	if err != nil {
		return 0, err
	}
	return server.Connection.WriteToUDP(data, addr)
}

func (server *Server) Close() {
	server.Connection.Close()
}

func (server *Server) EncodeMembersList() string {
	var arr = []string{}

	server.MemberLock.Lock()
	defer server.MemberLock.Unlock()

	for _, host := range server.Members {
		arr = append(arr, fmt.Sprintf("%s:%d:%d", host.Signature, host.Counter, host.UpdatedAt))
	}

	return strings.Join(arr, ";")
}

func (s *Server) GetJoinMessage() string {
	return fmt.Sprintf("JOIN %s\n", s.Signature)
}

func (s *Server) GetLeaveMessage() string {
	return fmt.Sprintf("LEAVE %s\n", s.Signature)
}

func (s *Server) GetPingMessage() string {
	return fmt.Sprintf("PING %s\n%s\n", s.Signature, s.EncodeMembersList())
}

