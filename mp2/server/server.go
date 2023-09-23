package server

import (
	"cs425/timer"
	"errors"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const GOSSIP_PROTOCOL = 0
const GOSPSIP_SUSPICION_PROTOCOL = 1

const T_GOSSIP = 2000 * time.Millisecond  // Time duration between each gossip round
const T_TIMEOUT = 4000 * time.Millisecond // Time duration until a peer times out
// const T_GOSSIP = 500 * time.Millisecond   // Time duration between each gossip round
// const T_TIMEOUT = 2500 * time.Millisecond // Time duration until a peer times out
const T_CLEANUP = 2 * T_TIMEOUT // Time duration before peer is deleted

const T_FINAL = 2 * T_TIMEOUT

type Host struct {
	Hostname  string
	Port      int
	Address   *net.UDPAddr
	Signature string
	ID        string
	Counter   int   // heartbeat counter
	UpdatedAt int64 // local timestamp when counter last updated
	Suspected bool  // whether the node is suspected of failure
	Failed    bool
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
	DropRate         int
	TotalByte        int
	TimerManager     *timer.TimerManager
	GossipPeriod     time.Duration
	SuspicionTimeout time.Duration
	GossipTimeout    time.Duration
	GossipChannel    chan bool
	ReceiverChannel  chan ReceiverEvent
	InputChannel     chan string
	Protocol         int
}

func NewHost(Hostname string, Port int, ID string, Address *net.UDPAddr) *Host {
	var host = &Host{}
	host.ID = ID
	host.Hostname = Hostname
	host.Port = Port
	host.Address = Address
	host.Signature = fmt.Sprintf("%s:%d:%s", Hostname, Port, ID)
	host.Counter = 0
	host.UpdatedAt = time.Now().UnixMilli()
	host.Suspected = false
	host.Failed = false
	return host
}

func (server *Server) SetUniqueID() string {
	server.MemberLock.Lock()
	defer server.MemberLock.Unlock()

	Timestamp := fmt.Sprintf("%d", time.Now().UnixNano())
	ID := fmt.Sprintf("%d%s", server.Self.Port, Timestamp[:16])
	server.Self.ID = ID
	server.Self.Signature = fmt.Sprintf("%s:%d:%s", server.Self.Hostname, server.Self.Port, ID)
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
	server.Active = false
	server.Connection = conn
	server.Members = make(map[string]*Host)
	server.TimerManager = timer.NewTimerManager()
	server.DropRate = 0
	server.TotalByte = 0
	server.GossipPeriod = T_GOSSIP
	server.GossipTimeout = T_TIMEOUT
	server.SuspicionTimeout = T_CLEANUP
	server.GossipChannel = make(chan bool)
	server.ReceiverChannel = make(chan ReceiverEvent)
	server.InputChannel = make(chan string)
	server.Protocol = GOSSIP_PROTOCOL

	server.SetUniqueID()

	return server, nil
}

func (server *Server) AddHost(Hostname string, Port int, ID string) (*Host, error) {
	server.MemberLock.Lock()
	defer server.MemberLock.Unlock()

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", Hostname, Port))
	if err != nil {
		return nil, err
	}

	if found, ok := server.Members[ID]; ok {
		log.Info("Duplicate host: ", found)
		return nil, errors.New("A peer with this ID already exists")
	}

	server.Members[ID] = NewHost(Hostname, Port, ID, addr)
	log.Warnf("Added new host: %s\n", server.Members[ID].Signature)

	server.MemberLock.Unlock()
	server.PrintMembershipTable()
	server.MemberLock.Lock()

	return server.Members[ID], nil
}

func (server *Server) GetPacket() (message string, addr *net.UDPAddr, err error) {
	buffer := make([]byte, 1024)
	n, addr, err := server.Connection.ReadFromUDP(buffer)
	if err != nil {
		return "", nil, err
	}
	message = strings.TrimSpace(string(buffer[:n]))
	log.Debugf("Received %d bytes from %s\n", len(message), addr.String())
	return message, addr, nil
}

func (server *Server) Close() {
	server.Connection.Close()
	server.TimerManager.Close()
	close(server.GossipChannel)
	close(server.ReceiverChannel)
	close(server.InputChannel)
}

func (server *Server) EncodeMembersList(hideSuspected bool) string {
	if server.Protocol == GOSPSIP_SUSPICION_PROTOCOL {
		return server.GS_encodeMembersList()
	}

	server.MemberLock.Lock()
	defer server.MemberLock.Unlock()

	var arr = []string{}
	for _, host := range server.Members {
		if hideSuspected && host.Suspected {
			continue
		}

		arr = append(arr, fmt.Sprintf("%s:%d:%d", host.Signature, host.Counter, host.UpdatedAt))
	}
	return strings.Join(arr, ";")
}

// func (server *Server) ProcessMembersList(message string, withRestartTimer bool) {
//
// 	if server.Protocol == GOSPSIP_SUSPICION_PROTOCOL {
// 		server.GS_processMembersList(message)
// 		return
// 	}
//
// 	server.MemberLock.Lock()
// 	defer server.MemberLock.Unlock()
//
// 	members := strings.Split(message, ";")
// 	for _, member := range members {
// 		tokens := strings.Split(member, ":")
// 		if len(tokens) < 4 {
// 			continue
// 		}
//
// 		timeNow := time.Now().UnixMilli()
// 		memberHost, memberPort, memberID, memberCounter := tokens[0], tokens[1], tokens[2], tokens[3]
//
// 		if memberID == server.Self.ID {
// 			continue
// 		}
//
// 		memberPortInt, err := strconv.Atoi(memberPort)
// 		if err != nil {
// 			continue
// 		}
//
// 		memberCounterInt, err := strconv.Atoi(memberCounter)
// 		if err != nil {
// 			continue
// 		}
//
// 		if _, ok := server.Members[memberID]; !ok {
// 			server.MemberLock.Unlock()
// 			server.AddHost(memberHost, memberPortInt, memberID)
// 			server.MemberLock.Lock()
// 		}
//
// 		found, _ := server.Members[memberID]
// 		if found.Counter < memberCounterInt {
// 			found.Counter = memberCounterInt
// 			found.UpdatedAt = timeNow
// 			found.Suspected = false
//
// 			if withRestartTimer {
// 				server.TimerManager.RestartTimer(memberID, server.GossipTimeout)
// 			}
// 		}
// 	}
// }

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
	return fmt.Sprintf("PING %s %s\n%s\n", s.Self.Signature, targetID, s.EncodeMembersList(true))
}

// pretty print membership table
func (s *Server) PrintMembershipTable() {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"ID", "ADDRESS", "COUNT", "UPDATED", "SUSPECTED", "FAILED"})

	rows := []table.Row{}

	for _, host := range s.Members {
		rows = append(rows, table.Row{
			host.ID, fmt.Sprintf("%s:%d", host.Hostname, host.Port),
			host.Counter, host.UpdatedAt, host.Suspected, host.Failed,
		})
	}

	t.SortBy([]table.SortBy{
		{Name: "COUNT", Mode: table.DscNumeric},
	})

	t.AppendRows(rows)
	t.AppendSeparator()
	t.SetStyle(table.StyleLight)
	t.Render()
}

func (server *Server) processRowWithGossipProcotol(tokens []string) {
	server.MemberLock.Lock()
	defer server.MemberLock.Unlock()

	if len(tokens) < 4 {
		return
	}

	timeNow := time.Now().UnixMilli()
	host, portStr, ID, countStr := tokens[0], tokens[1], tokens[2], tokens[3]

	if ID == server.Self.ID {
		return
	}

	port, _ := strconv.Atoi(portStr)
	count, _ := strconv.Atoi(countStr)

	found, ok := server.Members[ID]

	// new member
	if !ok {
		server.MemberLock.Unlock()
		server.AddHost(host, port, ID)
		server.MemberLock.Lock()

		if newMember, ok := server.Members[ID]; ok {
			newMember.Counter = count
			newMember.UpdatedAt = timeNow
			newMember.Suspected = false
			server.TimerManager.RestartTimer(ID, server.GossipTimeout)
		}

		return
	}

	// known member
	if found.Counter < count {
		found.Counter = count
		found.UpdatedAt = timeNow
		found.Suspected = false
		server.TimerManager.RestartTimer(ID, server.GossipTimeout)
	}
}

func (s *Server) processRowWithGossipSuspicionProcotol(tokens []string) {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	timeNow := time.Now().UnixMilli()
	host, portStr, ID, countStr, suspectedStr, failedStr := tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[5]
	port, _ := strconv.Atoi(portStr)
	count, _ := strconv.Atoi(countStr)

	var suspected bool = false
	var failed bool = false

	if suspectedStr == "true" {
		suspected = true
	}
	if failedStr == "true" {
		failed = true
	}

	if ID == s.Self.ID {
		if failed {
			// TODO: Restart Gossip with new ID...
			log.Fatal("Node has failed")
		} else {
			return
		}
	}

	found, ok := s.Members[ID]

	// Failure overrides everything else
	if failed {
		if ok {
			s.TimerManager.RestartTimer(ID, T_FINAL)
			found.Failed = true
		}
		return
	}

	// The member does not exist in our membership list
	if !ok {
		s.MemberLock.Unlock()
		s.AddHost(host, port, ID)
		s.MemberLock.Lock()

		if newMember, ok := s.Members[ID]; ok {
			newMember.Counter = count
			newMember.Suspected = suspected
			newMember.Failed = false
			newMember.UpdatedAt = timeNow

			if suspected {
				s.TimerManager.RestartTimer(ID, s.SuspicionTimeout)
			} else {
				s.TimerManager.RestartTimer(ID, s.GossipTimeout)
			}
		}
		return
	}

	// Member exists in memberhsip list and not failed
	if found.Counter == count {
		if !found.Suspected {
			found.Suspected = suspected
			found.Failed = false
			found.UpdatedAt = timeNow

			if suspected {
				s.TimerManager.RestartTimer(ID, s.SuspicionTimeout)
			} else {
				s.TimerManager.RestartTimer(ID, s.GossipTimeout)
			}
		}
	} else if found.Counter < count {
		found.Counter = count
		found.Suspected = suspected
		found.Failed = false
		found.UpdatedAt = timeNow

		if suspected {
			s.TimerManager.RestartTimer(ID, s.SuspicionTimeout)
		} else {
			s.TimerManager.RestartTimer(ID, s.GossipTimeout)
		}
	}

	// Ignore message if node not failed and has a bigger counter
}

func (s *Server) ProcessMembersList(message string) {
	members := strings.Split(message, ";")
	for _, member := range members {
		tokens := strings.Split(member, ":")
		if len(tokens) == 4 {
			s.processRowWithGossipProcotol(tokens)
		} else if len(tokens) == 6 {
			s.processRowWithGossipSuspicionProcotol(tokens)
		} else {
			log.Warn("Invalid number of tokens")
		}
	}
}

// Each member is encoded as "host:port:id:counter:suspected:failed"
func (s *Server) GS_encodeMembersList() string {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	var arr = []string{}
	for _, host := range s.Members {
		suspected := "false"
		failed := "false"

		if host.Suspected {
			suspected = "true"
		}

		if host.Failed {
			failed = "true"
		}

		arr = append(arr, fmt.Sprintf("%s:%d:%s:%d:%s:%s", host.Hostname, host.Port,
			host.ID, host.Counter, suspected, failed))
	}
	return strings.Join(arr, ";")
}
