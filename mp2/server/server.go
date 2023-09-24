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

const (
	GOSSIP_PROTOCOL            = 0
	GOSPSIP_SUSPICION_PROTOCOL = 1
)

const (
	NODE_ALIVE     = 0
	NODE_SUSPECTED = 1
	NODE_FAILED    = 2
)

const (
	// T_GOSSIP  = 300 * time.Millisecond
	// T_FAIL    = 1500 * time.Millisecond
	// T_CLEANUP = 3000 * time.Millisecond
	// T_SUSPECT = 1500 * time.Millisecond

	T_GOSSIP  = 300 * time.Millisecond
	T_FAIL    = 1500 * time.Millisecond
	T_CLEANUP = 3000 * time.Millisecond
	T_SUSPECT = 1500 * time.Millisecond
)

type Host struct {
	Hostname  string
	Port      int
	Address   *net.UDPAddr
	Signature string
	ID        string
	Counter   int   // heartbeat counter
	UpdatedAt int64 // local timestamp when counter last updated
	State     int
}

type ReceiverEvent struct {
	Message string
	Sender  *net.UDPAddr
}

type Server struct {
	Active          bool
	Self            *Host
	Connection      *net.UDPConn
	Members         map[string]*Host
	MemberLock      sync.Mutex
	DropRate        int
	TotalByte       int
	TimerManager    *timer.TimerManager
	GossipChannel   chan bool
	ReceiverChannel chan ReceiverEvent
	InputChannel    chan string
	Protocol        int
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
	host.State = NODE_ALIVE
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

// Each member is encoded as "host:port:id:counter:state"
func (server *Server) EncodeMembersList() string {
	server.MemberLock.Lock()
	defer server.MemberLock.Unlock()

	var arr = []string{}
	for _, host := range server.Members {
		arr = append(arr, fmt.Sprintf("%s:%d:%s:%d:%d", host.Hostname, host.Port, host.ID, host.Counter, host.State))
	}
	return strings.Join(arr, ";")
}

func (s *Server) GetJoinMessage() string {
	return fmt.Sprintf("JOIN %s\n", s.Self.Signature)
}

func (s *Server) GetPingMessage(targetID string) string {
	return fmt.Sprintf("PING %s %s\n%s\n", s.Self.Signature, targetID, s.EncodeMembersList())
}

func StateToString(state int) string {
	if state == NODE_ALIVE {
		return "alive"
	} else if state == NODE_SUSPECTED {
		return "suspected"
	} else if state == NODE_FAILED {
		return "failed"
	}
	return "unkown"
}

// pretty print membership table
func (s *Server) PrintMembershipTable() {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"ID", "ADDRESS", "COUNT", "UPDATED", "STATE"})

	rows := []table.Row{}

	for _, host := range s.Members {
		rows = append(rows, table.Row{
			host.ID, fmt.Sprintf("%s:%d", host.Hostname, host.Port),
			host.Counter, host.UpdatedAt, StateToString(host.State),
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

func (s *Server) RestartTimer(ID string, state int) {
	if state == NODE_ALIVE {
		if s.Protocol == GOSSIP_PROTOCOL {
			s.TimerManager.RestartTimer(ID, T_FAIL)
			log.Warnf("Failure timer for Gossip restarted at %d milliseconds\n", time.Now().UnixMilli())
		} else {
			log.Warnf("Suspected timer restarted at %d milliseconds\n", time.Now().UnixMilli())
			s.TimerManager.RestartTimer(ID, T_SUSPECT)
		}
	} else if state == NODE_SUSPECTED {
		s.TimerManager.RestartTimer(ID, T_FAIL)
		log.Warnf("Failure timer restarted at %d milliseconds\n", time.Now().UnixMilli())
	} else if state == NODE_FAILED {
		s.TimerManager.RestartTimer(ID, T_CLEANUP)
		log.Warnf("Cleanup timer restarted at %d milliseconds\n", time.Now().UnixMilli())
	}
}

// To merge membership tables
func (s *Server) processRow(tokens []string) {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	if len(tokens) < 5 {
		return
	}

	timeNow := time.Now().UnixMilli()

	host, portStr, ID, countStr, stateStr := tokens[0], tokens[1], tokens[2], tokens[3], tokens[4]
	port, _ := strconv.Atoi(portStr)
	count, _ := strconv.Atoi(countStr)
	state, _ := strconv.Atoi(stateStr)

	// Handle entry for current server
	if ID == s.Self.ID {
		if state == NODE_FAILED {
			// TODO: Restart Gossip with new ID.
			log.Fatalf("FALSE DETECTION: Node %s has failed", s.Self.Signature)
		}
		return
	}

	found, ok := s.Members[ID]

	// Do not add a failed node back
	if !ok && state == NODE_FAILED {
		return
	}

	// New member
	if !ok {
		s.MemberLock.Unlock()
		s.AddHost(host, port, ID)
		s.MemberLock.Lock()

		// Update new member
		if newMember, ok := s.Members[ID]; ok {
			newMember.Counter = count
			newMember.UpdatedAt = timeNow
			newMember.State = state
			s.RestartTimer(ID, state)
		}
		return
	}

	// failed state overrides everything
	if found.State == NODE_FAILED {
		return
	}

	// higher count overrides alive or suspected state
	if found.Counter < count {
		found.Counter = count
		found.UpdatedAt = timeNow
		found.State = state
		s.RestartTimer(ID, state)
		return
	}

	// within same counter, suspected or failed state overrides alive state
	if found.Counter == count && found.State == NODE_ALIVE && state != NODE_ALIVE {
		found.State = state
		found.UpdatedAt = timeNow
		s.RestartTimer(ID, state)
		return
	}
}

func (s *Server) ProcessMembersList(message string) {
	members := strings.Split(message, ";")
	for _, member := range members {
		tokens := strings.Split(member, ":")
		s.processRow(tokens)
	}
}
