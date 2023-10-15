package failuredetector

import (
	"cs425/common"
	"cs425/timer"
	"errors"
	"fmt"
	table "github.com/jedib0t/go-pretty/table"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	JOIN_OK               = "JOIN_OK"
	JOIN_ERROR            = "JOIN_ERROR"
	BAD_REQUEST           = "BAD_REQUEST"
	JOIN_TIMER_ID         = "JOIN_TIMER"
	ERROR_ILLEGAL_REQUEST = JOIN_ERROR + "\n" + "Illegal Request" + "\n"
)

var (
	T_GOSSIP  = 300 * time.Millisecond
	T_FAIL    = 4000 * time.Millisecond
	T_CLEANUP = 8000 * time.Millisecond
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
	Notifier        common.Notifier
}

var Logger *log.Logger = nil

func (s *Server) Start() {
	defer s.Close()

	Logger.Infof("Node %s: failure detector running on port %d\n", s.Self.ID, s.Self.Port)

	if IsIntroducer(s) {
		s.Active = true
		Logger.Info("Introducer is online...")
	}

	go receiverRoutine(s) // to receive requests from network
	go senderRoutine(s)   // to send gossip messages

	sendJoinRequest(s)

	// Blocks until either new message received or timer sends a signal
	for {
		select {
		case e := <-s.TimerManager.TimeoutChannel:
			s.HandleTimeout(e)
		case e := <-s.ReceiverChannel:
			s.HandleRequest(e)
		case e := <-s.InputChannel:
			s.HandleCommand(e)
		}
	}
}

func (s *Server) ChangeProtocol(protocol int) {
	if protocol == common.GOSPSIP_SUSPICION_PROTOCOL {
		s.Protocol = common.GOSPSIP_SUSPICION_PROTOCOL
		T_FAIL = 3000 * time.Millisecond
		T_CLEANUP = 6000 * time.Millisecond
	} else {
		s.Protocol = common.GOSSIP_PROTOCOL
		T_FAIL = 4000 * time.Millisecond
		T_CLEANUP = 8000 * time.Millisecond
	}
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
	host.State = common.NODE_ALIVE
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

func NewServer(Hostname string, Port int, Protocol int, Notifier common.Notifier) *Server {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", Port))
	if err != nil {
		Logger.Fatal(err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		Logger.Fatal(err)
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
	server.Protocol = Protocol
	server.Notifier = Notifier

	server.SetUniqueID()

	return server
}

func (server *Server) AddHost(Hostname string, Port int, ID string) (*Host, error) {
	server.MemberLock.Lock()
	defer server.MemberLock.Unlock()

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", Hostname, Port))
	if err != nil {
		return nil, err
	}

	if found, ok := server.Members[ID]; ok {
		Logger.Info("Duplicate host: ", found)
		return nil, errors.New("A peer with this ID already exists")
	}

	server.Members[ID] = NewHost(Hostname, Port, ID, addr)
	Logger.Warnf("Added new host: %s\n", server.Members[ID].Signature)

	server.MemberLock.Unlock()
	server.PrintMembershipTable()
	server.MemberLock.Lock()

	server.Notifier.HandleNodeJoin(common.GetNodeByAddress(Hostname, Port))

	return server.Members[ID], nil
}

func (server *Server) GetPacket() (message string, addr *net.UDPAddr, err error) {
	buffer := make([]byte, 1024)
	n, addr, err := server.Connection.ReadFromUDP(buffer)
	if err != nil {
		return "", nil, err
	}
	message = strings.TrimSpace(string(buffer[:n]))
	Logger.Debugf("Received %d bytes from %s\n", len(message), addr.String())
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
	if state == common.NODE_ALIVE {
		return "alive"
	} else if state == common.NODE_SUSPECTED {
		return "suspected"
	} else if state == common.NODE_FAILED {
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
	t.SetStyle(table.StyleLight)
	t.Render()
}

func (s *Server) RestartTimer(ID string, state int) {
	if state == common.NODE_ALIVE {
		if s.Protocol == common.GOSSIP_PROTOCOL {
			s.TimerManager.RestartTimer(ID, T_FAIL)
			// Logger.Warnf("Failure timer for Gossip restarted at %d milliseconds\n", time.Now().UnixMilli())
		} else {
			// Logger.Warnf("Suspected timer restarted at %d milliseconds\n", time.Now().UnixMilli())
			s.TimerManager.RestartTimer(ID, T_SUSPECT)
		}
	} else if state == common.NODE_SUSPECTED {
		s.TimerManager.RestartTimer(ID, T_FAIL)
		// Logger.Warnf("Failure timer restarted at %d milliseconds\n", time.Now().UnixMilli())
	} else if state == common.NODE_FAILED {
		s.TimerManager.RestartTimer(ID, T_CLEANUP)
		// Logger.Warnf("Cleanup timer restarted at %d milliseconds\n", time.Now().UnixMilli())
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
		if state == common.NODE_FAILED {
			// TODO: Restart Gossip with new ID.
			Logger.Fatalf("FALSE DETECTION: Node %s has failed", s.Self.Signature)
		}
		return
	}

	found, ok := s.Members[ID]

	// Do not add a failed node back
	if !ok && state == common.NODE_FAILED {
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
	if found.State == common.NODE_FAILED {
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
	if found.Counter == count && found.State == common.NODE_ALIVE && state != common.NODE_ALIVE {
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

// Fix the first node as the introducer
func IsIntroducer(s *Server) bool {
	return s.Self.Port == common.Cluster[0].UDPPort && s.Self.Hostname == common.Cluster[0].Hostname
}

// Sends membership list to random subset of peers every T_gossip period
// Updates own counter and timestamp before sending the membership list
func sendPings(s *Server) {
	targets := selectRandomTargets(s, common.NODES_PER_ROUND)
	if len(targets) == 0 {
		return
	}

	Logger.Debugf("Sending gossip to %d hosts", len(targets))

	s.MemberLock.Lock()
	s.Self.Counter++
	s.Self.UpdatedAt = time.Now().UnixMilli()
	s.MemberLock.Unlock()

	for _, target := range targets {
		message := s.GetPingMessage(target.ID)
		n, err := s.Connection.WriteToUDP([]byte(message), target.Address)
		if err != nil {
			Logger.Println(err)
			continue
		}
		s.TotalByte += n
		Logger.Debugf("Sent %d bytes to %s\n", n, target.Signature)
	}
}

// Selects at most 'count' number of hosts from list
func selectRandomTargets(s *Server, count int) []*Host {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	var hosts = []*Host{}
	for _, host := range s.Members {
		if host.ID != s.Self.ID {
			hosts = append(hosts, host)
		}
	}

	// shuffle the array
	for i := len(hosts) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		hosts[i], hosts[j] = hosts[j], hosts[i]
	}

	if len(hosts) < count {
		return hosts
	}
	return hosts[:count]
}

// Timeout signal received from timer
// Either suspect node or mark failed
func (s *Server) HandleTimeout(e timer.TimerEvent) {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	// should never happen btw
	if e.ID == s.Self.ID {
		return
	}

	if e.ID == JOIN_TIMER_ID {
		if IsIntroducer(s) {
			if len(s.Members) <= 1 {
				Logger.Info("Timeout: Retrying JOIN.")
				sendJoinRequest(s)
			}
		} else if !s.Active {
			Logger.Info("Timeout: Retrying JOIN.")
			sendJoinRequest(s)
		}

		return
	}

	host, ok := s.Members[e.ID]

	// Ignore timeout for node which does not exist
	if !ok {
		return
	}

	// currentTime := time.Now()
	// // Format the current time as "hour:minute:second:millisecond"
	// timestamp := currentTime.Format("15:04:05:000")
	timestamp := time.Now().UnixMilli()

	if host.State == common.NODE_ALIVE {
		if s.Protocol == common.GOSSIP_PROTOCOL {
			Logger.Warnf("FAILURE DETECTED: (%d) Node %s is considered failed\n", timestamp, host.Signature)
			host.State = common.NODE_FAILED
			s.Notifier.HandleNodeLeave(common.GetNodeByAddress(host.Hostname, host.Port))
		} else {
			Logger.Warnf("FAILURE SUSPECTED: (%d) Node %s is suspected of failure\n", timestamp, host.Signature)
			host.State = common.NODE_SUSPECTED
		}
		s.RestartTimer(e.ID, host.State)
	} else if host.State == common.NODE_SUSPECTED {
		Logger.Warnf("FAILURE DETECTED: (%d) Node %s is considered failed\n", timestamp, host.Signature)
		host.State = common.NODE_FAILED
		s.RestartTimer(e.ID, host.State)
		s.Notifier.HandleNodeLeave(common.GetNodeByAddress(host.Hostname, host.Port))
	} else if host.State == common.NODE_FAILED {
		Logger.Warn("Deleting node from membership list...", host.Signature)
		delete(s.Members, e.ID)
	}

	s.MemberLock.Unlock()
	s.PrintMembershipTable()
	s.MemberLock.Lock()
}

// This routine gossips the membership list every GossipPeriod.
// It listens for start/stop signals on the GossipChannel.
func senderRoutine(s *Server) {
	active := true

	for {
		select {
		case active = <-s.GossipChannel:
			if active {
				Logger.Info("Starting gossip...")
			} else {
				Logger.Info("Stopping gossip...")
			}
		case <-time.After(T_GOSSIP):
			break
		}

		if active {
			// ghostEntryRemover(s)
			sendPings(s)
		}
	}
}

// This routine listens for messages to the server and forwards them to the ReceiverChannel.
func receiverRoutine(s *Server) {
	for {
		message, sender, err := s.GetPacket()
		if err != nil {
			Logger.Error(err)
			continue
		}

		s.ReceiverChannel <- ReceiverEvent{Message: message, Sender: sender}

	}
}

func startGossip(s *Server) {
	if IsIntroducer(s) {
		Logger.Warn("Introducer is always active")
		return
	}

	if s.Active {
		Logger.Warn("server is already active")
		return
	}

	ID := s.SetUniqueID()
	Logger.Debugf("Updated Node ID to %s", ID)
	s.GossipChannel <- true
	sendJoinRequest(s)
}

func stopGossip(s *Server) {
	if IsIntroducer(s) {
		Logger.Warn("Introducer is always active")
		return
	}

	if !s.Active {
		Logger.Warn("server is already inactive")
		return
	}

	s.Active = false
	s.GossipChannel <- false
	s.MemberLock.Lock()
	for ID := range s.Members {
		delete(s.Members, ID)
	}
	s.MemberLock.Unlock()
	s.TimerManager.StopAll()
}

// Send request to join node and start timer
func sendJoinRequest(s *Server) {
	msg := s.GetJoinMessage()

	if IsIntroducer(s) {
		for _, vm := range common.Cluster {
			if vm.Hostname == s.Self.Hostname && vm.UDPPort == s.Self.Port {
				continue
			}

			addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", vm.Hostname, vm.UDPPort))
			if err != nil {
				Logger.Fatal(err)
			}

			_, err = s.Connection.WriteToUDP([]byte(msg), addr)
			if err != nil {
				Logger.Println(err)
				continue
			}

			Logger.Printf("Sent join request to %s:%d\n", vm.Hostname, vm.UDPPort)
		}

	} else {

		introducer := common.Cluster[0]

		addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", introducer.Hostname, introducer.UDPPort))
		if err != nil {
			Logger.Fatal(err)
		}

		s.Connection.WriteToUDP([]byte(msg), addr)
		Logger.Printf("Sent join request to %s:%d\n", introducer.Hostname, introducer.UDPPort)
	}

	s.TimerManager.RestartTimer(JOIN_TIMER_ID, common.JOIN_RETRY_TIMEOUT)
}

func (s *Server) HandleCommand(command string) {
	commands := []string{"ls: print membership table", "id: print id of node",
		"kill: crash server", "join: start gossiping", "leave: stop gossiping", "sus (on|off): enable/disable gossip suspicion protocol",
		"help: list all commands"}

	switch strings.ToLower(command) {

	case "ls":
		s.PrintMembershipTable()

	case "id":
		fmt.Println(s.Self.ID)

	case "kill":
		Logger.Fatalf("Kill command received at %d milliseconds", time.Now().UnixMilli())

	case "start_gossip":
		fallthrough

	case "join":
		startGossip(s)
		fmt.Println("OK")

	case "stop_gossip":
		fallthrough

	case "leave":
		stopGossip(s)
		fmt.Println("OK")

	case "help":
		for i := range commands {
			fmt.Printf("%d. %s\n", i+1, commands[i])
		}
	}
}

// Handles the request received by the server
// JOIN, PING, ID, LIST, KILL, START_GOSSIP, STOP_GOSSIP, CONFIG, SUS ON, SUS OFF, LIST_SUS
func (s *Server) HandleRequest(e ReceiverEvent) {
	commands := []string{"ls: print membership table", "id: print id of node",
		"kill: crash server", "start_gossip: start gossiping", "stop_gossip: stop gossiping", "sus <on|off>: toggle gossip suspicion protocol",
		"config <option> [<value>]: get/set config parameter", "help: list all commands"}

	lines := strings.Split(e.Message, "\n")
	if len(lines) < 1 {
		return
	}

	header := lines[0]
	tokens := strings.Split(header, " ")

	Logger.Debugf("Request %s received from: %v\n", tokens[0], e.Sender)

	switch verb := strings.ToLower(tokens[0]); verb {
	case "join":
		HandleJoinRequest(s, e)

	case "join_ok":
		HandleJoinResponse(s, e)

	case "join_error":
		Logger.Warnf("Failed to join: %s", e.Message)

	case "ping":
		HandlePingRequest(s, e)

	case "id":
		s.Connection.WriteToUDP([]byte(fmt.Sprintf("%s\n", s.Self.ID)), e.Sender)

	case "ls":
		s.Connection.WriteToUDP([]byte(fmt.Sprintf("OK\n%s\n", strings.ReplaceAll(s.EncodeMembersList(), ";", "\n"))), e.Sender)

	case "kill":
		Logger.Fatalf("KILL command received at %d milliseconds", time.Now().UnixMilli())

	case "start_gossip":
		startGossip(s)
		Logger.Warnf("START command received at %d milliseconds", time.Now().UnixMilli())
		s.Connection.WriteToUDP([]byte("OK\n"), e.Sender)

	case "stop_gossip":
		stopGossip(s)
		Logger.Warnf("STOP command received at %d milliseconds", time.Now().UnixMilli())
		s.Connection.WriteToUDP([]byte("OK\n"), e.Sender)

	case "config":
		HandleConfigRequest(s, e)

	case "sus":
		HandleSusRequest(s, e)

	case "help":
		s.Connection.WriteToUDP([]byte(strings.Join(commands, "\n")), e.Sender)

	default:
		Logger.Warn("Unknown request verb: ", verb)
	}
}

func HandleJoinResponse(s *Server, e ReceiverEvent) {
	lines := strings.Split(e.Message, "\n")
	if len(lines) < 2 || (s.Active && !IsIntroducer(s)) {
		return
	}

	Logger.Info("Join accepted by ", e.Sender)

	s.TimerManager.StopTimer(JOIN_TIMER_ID)
	s.ProcessMembersList(lines[1])
	// s.StartAllTimers()
	s.Active = true
	Logger.Info("Node join completed.")
}

// Handle config command: CONFIG <field to change> <value>
func HandleConfigRequest(s *Server, e ReceiverEvent) {
	words := strings.Split(e.Message, " ")

	if words[1] == "DROPRATE" {
		if len(words) == 2 {
			s.Connection.WriteToUDP([]byte(fmt.Sprintf("DROPRATE %d\n", s.DropRate)), e.Sender)
		} else if len(words) == 3 {
			dropRate, err := strconv.Atoi(words[2])
			if err != nil {
				return
			}
			s.DropRate = dropRate
			s.Connection.WriteToUDP([]byte(fmt.Sprintf("OK\n")), e.Sender)
		}
	}

	if words[1] == "T_GOSSIP" {
		if len(words) == 2 {
			s.Connection.WriteToUDP([]byte(fmt.Sprintf("T_GOSSIP %d ms\n", T_GOSSIP.Milliseconds())), e.Sender)
		} else if len(words) == 3 {
			value, err := strconv.Atoi(words[2])
			if err != nil {
				return
			}
			T_GOSSIP = time.Duration(value) * time.Millisecond
			s.Connection.WriteToUDP([]byte(fmt.Sprintf("OK\n")), e.Sender)
		}
	}

	if words[1] == "T_FAIL" {
		if len(words) == 2 {
			s.Connection.WriteToUDP([]byte(fmt.Sprintf("T_FAIL %d ms\n", T_FAIL.Milliseconds())), e.Sender)
		} else if len(words) == 3 {
			value, err := strconv.Atoi(words[2])
			if err != nil {
				return
			}
			T_FAIL = time.Duration(value) * time.Millisecond
			s.Connection.WriteToUDP([]byte(fmt.Sprintf("OK\n")), e.Sender)
		}
	}

	if words[1] == "T_SUSPECT" {
		if len(words) == 2 {
			s.Connection.WriteToUDP([]byte(fmt.Sprintf("T_SUSPECT %d ms\n", T_SUSPECT.Milliseconds())), e.Sender)
		} else if len(words) == 3 {
			value, err := strconv.Atoi(words[2])
			if err != nil {
				return
			}
			T_SUSPECT = time.Duration(value) * time.Millisecond
			s.Connection.WriteToUDP([]byte(fmt.Sprintf("OK\n")), e.Sender)
		}
	}

	if words[1] == "T_CLEANUP" {
		if len(words) == 2 {
			s.Connection.WriteToUDP([]byte(fmt.Sprintf("T_CLEANUP %d ms\n", T_CLEANUP.Milliseconds())), e.Sender)
		} else if len(words) == 3 {
			value, err := strconv.Atoi(words[2])
			if err != nil {
				return
			}
			T_CLEANUP = time.Duration(value) * time.Millisecond
			s.Connection.WriteToUDP([]byte(fmt.Sprintf("OK\n")), e.Sender)
		}
	}
}

// Received member list from peer
func HandlePingRequest(s *Server, e ReceiverEvent) {
	if !s.Active {
		Logger.Debugf("PING from %s dropped as server is inactive\n", e)
		return
	}

	lines := strings.Split(e.Message, "\n")
	if len(lines) < 2 {
		return
	}

	tokens := strings.Split(lines[0], " ")
	if len(tokens) < 3 {
		Logger.Debugf("Illegal header for PING request: %s\n", lines[0])
		return
	}

	if rand.Intn(100) < s.DropRate {
		Logger.Debugf("PING from %s dropped with drop rate %d %%\n", e, s.DropRate)
		return
	}

	if tokens[2] != s.Self.ID {
		Logger.Debugf("Dropped PING due to ID mismatch: %s\n", tokens[2])
		return
	}

	s.ProcessMembersList(lines[1])
}

// Change gossip protocol
func HandleSusRequest(s *Server, e ReceiverEvent) {
	lines := strings.Split(e.Message, "\n")
	tokens := strings.Split(lines[0], " ")
	if len(tokens) < 2 {
		s.Connection.WriteToUDP([]byte("ERROR\n"), e.Sender)
		return
	}

	if strings.ToUpper(tokens[1]) == "ON" {
		s.ChangeProtocol(common.GOSPSIP_SUSPICION_PROTOCOL)
	} else if strings.ToUpper(tokens[1]) == "OFF" {
		s.ChangeProtocol(common.GOSSIP_PROTOCOL)
	}

	s.Connection.WriteToUDP([]byte("OK\n"), e.Sender)
}

// Function to handle the Join request by new node at any node
func HandleJoinRequest(s *Server, e ReceiverEvent) {
	if !s.Active && !IsIntroducer(s) {
		return
	}

	message := e.Message
	lines := strings.Split(message, "\n")
	tokens := strings.Split(lines[0], " ")
	if len(tokens) < 2 {
		s.Connection.WriteToUDP([]byte(ERROR_ILLEGAL_REQUEST), e.Sender)
		return
	}

	senderInfo := tokens[1]
	tokens = strings.Split(senderInfo, ":")
	if len(tokens) < 3 {
		s.Connection.WriteToUDP([]byte(ERROR_ILLEGAL_REQUEST), e.Sender)
		return
	}

	senderAddress, senderPort, senderId := tokens[0], tokens[1], tokens[2]
	senderPortInt, err := strconv.Atoi(senderPort)
	if err != nil {
		s.Connection.WriteToUDP([]byte(ERROR_ILLEGAL_REQUEST), e.Sender)
		return
	}

	host, err := s.AddHost(senderAddress, senderPortInt, senderId)
	if err != nil {
		Logger.Errorf("Failed to add host: %s\n", err.Error())
		reply := fmt.Sprintf("%s\n%s\n", JOIN_ERROR, err.Error())
		s.Connection.WriteToUDP([]byte(reply), e.Sender)
		return
	}

	reply := fmt.Sprintf("%s\n%s\n", JOIN_OK, s.EncodeMembersList())
	_, err = s.Connection.WriteToUDP([]byte(reply), host.Address)
	if err != nil {
		Logger.Error(err)
	}
}
