package server

import (
	"cs425/timer"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
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
	Self         *Host
	Connection   *net.UDPConn
	Members      map[string]*Host
	MemberLock   sync.Mutex
	Introducer   bool
	TimerManager *timer.TimerManager
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

	server := &Server{}

	server.Self = NewHost(Hostname, Port, ID, addr)
	server.Connection = conn
	server.Members = make(map[string]*Host)
	server.Introducer = false
	server.TimerManager = timer.NewTimerManager()
	server.Members[ID] = server.Self // Add server to its own membership list

	return server, nil
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

func (server *Server) ProcessMembersList(message string) {
	server.MemberLock.Lock()

	members := strings.Split(message, ";")

	for _, member := range members {
		tokens := strings.Split(member, ":")
		if len(tokens) < 4 {
			continue
		}

		timeNow := time.Now().UnixMilli()
		memberHost, memberPort, memberID, memberCounter := tokens[0], tokens[1], tokens[2], tokens[3]

		memberPortInt, err := strconv.Atoi(memberPort)
		if err != nil {
			continue
		}

		memberCounterInt, err := strconv.Atoi(memberCounter)
		if err != nil {
			continue
		}

		if _, ok := server.Members[memberID]; !ok {
			server.MemberLock.Unlock()
			server.AddHost(memberHost, memberPortInt, memberID)
			server.MemberLock.Lock()
		}

		found, _ := server.Members[memberID]
		if found.Counter < memberCounterInt {
			found.Counter = memberCounterInt
			found.UpdatedAt = timeNow
			found.Suspected = false

			if memberID != server.Self.ID {
				server.TimerManager.RestartTimer(memberID, timer.T_TIMEOUT)
			}
		}
	}

	server.MemberLock.Unlock()
}

func (s *Server) GetJoinMessage() string {
	return fmt.Sprintf("JOIN %s\n", s.Self.Signature)
}

func (s *Server) GetLeaveMessage() string {
	return fmt.Sprintf("LEAVE %s\n", s.Self.Signature)
}

func (s *Server) GetPingMessage() string {
	return fmt.Sprintf("PING %s\n%s\n", s.Self.Signature, s.EncodeMembersList())
}
