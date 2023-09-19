package server

import (
	"cs425/timer"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const T_GOSSIP = 5 * time.Second   // Time duration between each gossip round
const T_TIMEOUT = 10 * time.Second // Time duration until a peer times out
const T_CLEANUP = 5 * time.Second  // Time duration before peer is deleted

const SAVE_FILENAME = "known_hosts"

type Host struct {
	Hostname  string
	Port      int
	Address   *net.UDPAddr
	Signature string
	ID        string
	Counter   int   // heartbeat counter
	UpdatedAt int64 // local timestamp when counter last updated
	Suspected bool  // whether the node is suspected of failure
}

type ReceiverEvent struct {
	Message string
	Sender  *net.UDPAddr
}

type Server struct {
	Active           bool
	Self             *Host
	Connection       *net.UDPConn
	Members          map[string]*Host
	MemberLock       sync.Mutex
	Introducer       bool
	DropRate         int
	TotalByte        int
	TimerManager     *timer.TimerManager
	GossipPeriod     time.Duration
	SuspicionTimeout time.Duration
	GossipTimeout    time.Duration
	GossipChannel    chan bool
	ReceiverChannel  chan ReceiverEvent
}

func NewHost(Hostname string, Port int, ID string, Address *net.UDPAddr) *Host {
	var host = &Host{}
	host.ID = ID
	host.Hostname = Hostname
	host.Port = Port
	host.Address = Address
	host.Signature = fmt.Sprintf("%s:%d:%s", Hostname, Port, ID)
	host.Counter = 0
	host.UpdatedAt = 0
	return host
}

func (server *Server) SetUniqueID() string {
	Timestamp := fmt.Sprintf("%d", time.Now().UnixNano())
	ID := fmt.Sprintf("%d%s", server.Self.Port, Timestamp[:16])
	server.Self.ID = ID
	server.Self.Signature = fmt.Sprintf("%s:%d:%s", server.Self.Hostname, server.Self.Port, ID)
	server.MemberLock.Lock()
	defer server.MemberLock.Unlock()
	server.Members[ID] = server.Self
	return ID
}

func NewServer(Hostname string, Port int) (*Server, error) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", Port))
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	server := &Server{}

	server.Self = NewHost(Hostname, Port, "", addr)
	server.SetUniqueID()

	server.Active = true
	server.Connection = conn
	server.Members = make(map[string]*Host)
	server.Introducer = false
	server.TimerManager = timer.NewTimerManager()
	server.DropRate = 0
	server.TotalByte = 0
	server.Members[server.Self.ID] = server.Self // Add server to its own membership list
	server.GossipPeriod = T_GOSSIP
	server.GossipTimeout = T_TIMEOUT
	server.SuspicionTimeout = T_CLEANUP
	server.GossipChannel = make(chan bool)
	server.ReceiverChannel = make(chan ReceiverEvent)

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
		log.Info("Duplicate host: ", found)
		return nil, errors.New("A peer with this ID already exists")
	}

	server.Members[ID] = NewHost(Hostname, Port, ID, addr)
	log.Warn(fmt.Sprintf("Added new host: %s\n", server.Members[ID].Signature))
	if server.Introducer {
		server.MemberLock.Unlock()
		server.SaveMembersToFile()
		server.MemberLock.Lock()
	}
	return server.Members[ID], nil
}

// TODO: FIX THIS FUNCTION!!!!
func (s *Server) SaveMembersToFile() {
	save_file, err := os.OpenFile(SAVE_FILENAME, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("Failed to open file: %s\n", err.Error())
	}
	defer save_file.Close()
	_, err = save_file.WriteString(s.EncodeMembersList() + "\n")
	if err != nil {
		log.Fatalf("Failed to write to file: %s\n", err.Error())
	}

	log.Debug("Updated membership list in file")
}

func (server *Server) GetPacket() (message string, addr *net.UDPAddr, err error) {
	buffer := make([]byte, 1024)
	n, addr, err := server.Connection.ReadFromUDP(buffer)
	if err != nil {
		return "", nil, err
	}
	message = strings.TrimSpace(string(buffer[:n]))
	log.Debug(fmt.Sprintf("Received %d bytes from %s\n", len(message), addr.String()))
	return message, addr, nil
}

// func (server *Server) SendPacket(address string, port int, data []byte) (int, error) {
// 	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", address, port))
// 	if err != nil {
// 		return 0, err
// 	}
// 	return server.Connection.WriteToUDP(data, addr)
// }

func (server *Server) Close() {
	server.Connection.Close()
	server.TimerManager.Close()
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
				server.TimerManager.RestartTimer(memberID, server.GossipTimeout)
			}
		}
	}

	server.MemberLock.Unlock()
}

func (server *Server) StartAllTimers() {
	server.MemberLock.Lock()
	defer server.MemberLock.Unlock()
	for ID := range server.Members {
		if ID != server.Self.ID {
			server.TimerManager.RestartTimer(ID, server.GossipTimeout)
		}
	}
}

func (s *Server) GetJoinMessage() string {
	return fmt.Sprintf("JOIN %s\n", s.Self.Signature)
}

func (s *Server) GetLeaveMessage() string {
	return fmt.Sprintf("LEAVE %s\n", s.Self.Signature)
}

func (s *Server) GetPingMessage(targetID string) string {
	return fmt.Sprintf("PING %s %s\n%s\n", s.Self.Signature, targetID, s.EncodeMembersList())
}
