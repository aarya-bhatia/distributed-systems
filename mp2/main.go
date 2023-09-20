package main

import (
	"bufio"
	"cs425/server"
	"cs425/timer"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	NODES_PER_ROUND       = 2           // Number of random peers to send gossip every round
	INTRODUCER_HOST       = "127.0.0.1" // TODO: Update this with VM1 in prod
	INTRODUCER_PORT       = 6001
	JOIN_OK               = "JOIN_OK"
	JOIN_ERROR            = "JOIN_ERROR"
	ERROR_ILLEGAL_REQUEST = JOIN_ERROR + "\n" + "Illegal Request" + "\n"
	JOIN_RETRY_TIMEOUT    = time.Second * 10
	JOIN_TIMER_ID         = "JOIN_TIMER"
)

// Starts a UDP server on specified port
func main() {
	if len(os.Args) < 3 {
		program := filepath.Base(os.Args[0])
		log.Fatalf("Usage: %s <hostname> <port>", program)
	}

	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})

	log.SetReportCaller(false)
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)

	hostname := os.Args[1]
	var port int
	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

	s, err := server.NewServer(hostname, port)
	if err != nil {
		log.Fatal(err)
	}

	// log.SetLevel(log.DebugLevel)
	// if os.Getenv("DEBUG") != "TRUE" {
	// 	log.SetLevel(log.InfoLevel)
	// 	logfile := fmt.Sprintf("%s.log", s.Self.Signature)
	// 	f, err := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	defer f.Close()
	// 	log.SetOutput(f)
	// 	os.Stderr.WriteString(fmt.Sprintf("Log File: %s\n", logfile))
	// }

	log.Infof("Server %s listening on port %d\n", s.Self.Signature, port)
	defer s.Close()

	if port == INTRODUCER_PORT {
		s.Introducer = true
		s.Active = true
		loadKnownHosts(s)
		startNode(s)
		return
	}

	s.Introducer = false
	s.Active = false
	sendJoinRequest(s)
	startNode(s)
}

// pretty print membership table
func printMembershipTable(s *server.Server) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"ID", "HOSTNAME", "PORT", "COUNTER", "UPDATED_AT", "SUSPECTED"})
	rows := []table.Row{}
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()
	for _, host := range s.Members {
		t := time.Unix(0, host.UpdatedAt).Format("2006-01-02 15:04:05 MST")
		rows = append(rows, table.Row{host.ID, host.Hostname, host.Port, host.Counter, t, host.Suspected})
	}
	t.AppendRows(rows)
	t.AppendSeparator()
	t.SetStyle(table.StyleLight)
	t.Render()
}

// Sends membership list to random subset of peers every T_gossip period
// Updates own counter and timestamp before sending the membership list
func sendPings(s *server.Server) {
	targets := selectRandomTargets(s, NODES_PER_ROUND)
	if len(targets) == 0 {
		return
	}

	log.Infof("Sending gossip to %d hosts", len(targets))
	s.MemberLock.Lock()
	s.Self.Counter++
	s.Self.UpdatedAt = time.Now().UnixMilli()
	s.MemberLock.Unlock()

	for _, target := range targets {
		message := s.GetPingMessage(target.ID)
		n, err := s.Connection.WriteToUDP([]byte(message), target.Address)
		if err != nil {
			log.Println(err)
			continue
		}
		s.TotalByte += n
		log.Debugf("Sent %d bytes to %s\n", n, target.Signature)
	}
}

// Selects at most 'count' number of hosts from list
func selectRandomTargets(s *server.Server, count int) []*server.Host {
	var hosts = []*server.Host{}
	s.MemberLock.Lock()
	for _, host := range s.Members {
		if host.ID != s.Self.ID {
			hosts = append(hosts, host)
		}
	}
	s.MemberLock.Unlock()

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
// Updates the known_hosts file for introducer
func handleTimeout(s *server.Server, e timer.TimerEvent) {
	s.MemberLock.Lock()

	if e.ID == s.Self.ID {
		s.MemberLock.Unlock()
		return
	}

	if e.ID == JOIN_TIMER_ID && !s.Active {
		log.Info("Timeout: Retrying JOIN.")
		sendJoinRequest(s)
		s.TimerManager.RestartTimer(e.ID, JOIN_RETRY_TIMEOUT)
		return
	}

	if host, ok := s.Members[e.ID]; ok {
		if host.Suspected || s.SuspicionTimeout == 0 {
			log.Warnf("FAILURE DETECTED: Node %s is considered failed\n", e.ID)
			delete(s.Members, e.ID)
			if s.Introducer {
				s.MemberLock.Unlock()
				s.SaveMembersToFile()
				s.MemberLock.Lock()
			}
		} else {
			log.Warnf("FAILURE SUSPECTED: Node %s is suspected of failure\n", e.ID)
			host.Suspected = true
			s.TimerManager.RestartTimer(e.ID, s.SuspicionTimeout)
		}

		s.MemberLock.Unlock()
		printMembershipTable(s)
		return
	}

	s.MemberLock.Unlock()

}

// Handle config command: CONFIG <field to change> <value>
func handleConfigRequest(s *server.Server, e server.ReceiverEvent) {
	words := strings.Split(e.Message, " ")
	if len(words) < 3 {
		return
	}
	if words[1] == "DROPRATE" {
		dropRate, err := strconv.Atoi(words[2])
		if err != nil {
			return
		}
		s.DropRate = dropRate
	}
}

// This routine gossips the membership list every GossipPeriod.
// It listens for start/stop signals on the GossipChannel.
func senderRoutine(s *server.Server) {
	active := true

	for {
		select {
		case active = <-s.GossipChannel:
			if active {
				log.Info("Starting gossip...")
			} else {
				log.Info("Stopping gossip...")
			}
		case <-time.After(s.GossipPeriod):
			break
		}

		if active {
			sendPings(s)
		}
	}
}

// This routine listens for messages to the server and forwards them to the ReceiverChannel.
func receiverRoutine(s *server.Server) {
	for {
		message, sender, err := s.GetPacket()
		if err != nil {
			log.Error(err)
			continue
		}

		s.ReceiverChannel <- server.ReceiverEvent{Message: message, Sender: sender}

	}
}

func startGossip(s *server.Server) {
	ID := s.SetUniqueID()
	log.Debugf("Updated Node ID to %s", ID)
	s.GossipChannel <- true
	sendJoinRequest(s)
}

func stopGossip(s *server.Server) {
	s.Active = false
	s.GossipChannel <- false
	s.MemberLock.Lock()
	for ID := range s.Members {
		delete(s.Members, ID)
	}
	s.MemberLock.Unlock()
	s.TimerManager.StopAll()
}

func handlePingRequest(s *server.Server, e server.ReceiverEvent) {
	if !s.Active {
		log.Debugf("PING from %s dropped as server is inactive\n", e)
		return
	}

	lines := strings.Split(e.Message, "\n")
	if len(lines) < 2 {
		return
	}

	tokens := strings.Split(lines[0], " ")
	if len(tokens) < 3 {
		log.Debugf("Illegal header for PING request: %s\n", lines[0])
		return
	}

	if rand.Intn(100) < s.DropRate {
		log.Debugf("PING from %s dropped with drop rate %d %%\n", e, s.DropRate)
		return
	}

	if tokens[2] != s.Self.ID {
		log.Debugf("Dropped PING due to ID mismatch: %s\n", tokens[2])
		return
	}

	s.ProcessMembersList(lines[1], true)
}

func handleListSus(s *server.Server, e server.ReceiverEvent) {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()
	susMembers := []string{}

	for _, host := range s.Members {
		if host.Suspected {
			susMembers = append(susMembers, host.Signature)
		}
	}

	reply := fmt.Sprintf("OK\n%s\n", strings.Join(susMembers, "\n"))
	s.Connection.WriteToUDP([]byte(reply), e.Sender)
}

func handleSusRequest(s *server.Server, e server.ReceiverEvent) {
	lines := strings.Split(e.Message, "\n")
	tokens := strings.Split(lines[0], " ")
	if len(tokens) < 2 {
		return
	}
	if strings.ToUpper(tokens[1]) == "ON" {
		s.SuspicionTimeout = server.T_CLEANUP
	} else if strings.ToUpper(tokens[1]) == "OFF" {
		s.SuspicionTimeout = 0
	}
}

// Handles the request received by the server
// JOIN, PING, ID, LIST, KILL, START_GOSSIP, STOP_GOSSIP, CONFIG, SUS ON, SUS OFF, LIST_SUS
func handleRequest(s *server.Server, e server.ReceiverEvent) {
	lines := strings.Split(e.Message, "\n")
	if len(lines) < 1 {
		return
	}

	header := lines[0]
	tokens := strings.Split(header, " ")

	log.Debugf("Request %s received from: %v\n", tokens[0], e.Sender)

	switch verb := strings.ToUpper(tokens[0]); verb {
	case "JOIN":
		handleJoinRequest(s, e)

	case "JOIN_OK":
		handleJoinResponse(s, e)

	case "JOIN_ERROR":
		log.Fatalf("Failed to join: %s", e.Message)

	case "PING":
		handlePingRequest(s, e)

	case "ID":
		s.Connection.WriteToUDP([]byte(fmt.Sprintf("%s\n", s.Self.ID)), e.Sender)

	case "LIST":
		s.Connection.WriteToUDP([]byte(fmt.Sprintf("OK\n%s\n", strings.ReplaceAll(s.EncodeMembersList(), ";", "\n"))), e.Sender)

	case "KILL":
		log.Fatalf("Kill request received\n")

	case "START_GOSSIP":
		startGossip(s)

	case "STOP_GOSSIP":
		stopGossip(s)

	case "CONFIG":
		handleConfigRequest(s, e)

	case "SUS":
		handleSusRequest(s, e)

	case "LIST_SUS":
		handleListSus(s, e)

	default:
		log.Warn("Unknown request verb: ", verb)
	}
}

// Listen for commands on stdin
func inputRoutine(s *server.Server) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		s.InputChannel <- line
	}

	if err := scanner.Err(); err != nil {
		log.Warn("Error reading from stdin:", err)
	}
}

func handleCommand(s *server.Server, command string) {
	commands := []string{"list_mem: print membership table", "list_self: print id of node",
		"kill: crash server", "join: start gossiping", "leave: stop gossiping", "sus_on: enable gossip suspicion",
		"sus_off: disable gossip suspicion", "help: list all commands"}

	switch strings.ToLower(command) {
	case "ls":
		fallthrough
	case "list_mem":
		printMembershipTable(s)
	case "id":
		fallthrough
	case "list_self":
		fmt.Println(s.Self.ID)
	case "kill":
		log.Fatalf("Kill command received!")
	case "join":
		startGossip(s)
		fmt.Println("OK")
	case "leave":
		stopGossip(s)
		fmt.Println("OK")
	case "sus_on":
		s.SuspicionTimeout = server.T_CLEANUP
		fmt.Printf("Suspicion Timeout: %f sec\n", s.SuspicionTimeout.Seconds())
	case "sus_off":
		s.SuspicionTimeout = 0
		fmt.Println("OK")
	case "help":
		for i := range commands {
			fmt.Printf("%d. %s\n", i+1, commands[i])
		}
	}
}

// Send request to join node and start timer
func sendJoinRequest(s *server.Server) {
	if s.Introducer || s.Active {
		return
	}

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", INTRODUCER_HOST, INTRODUCER_PORT))
	if err != nil {
		log.Fatal(err)
	}

	_, err = s.Connection.WriteToUDP([]byte(s.GetJoinMessage()), addr)
	if err != nil {
		log.Println(err)
		return
	}

	s.TimerManager.StartTimer(JOIN_TIMER_ID, JOIN_RETRY_TIMEOUT)

	log.Println("Sent join request!")

}

func handleJoinResponse(s *server.Server, e server.ReceiverEvent) {
	if s.Introducer {
		return
	}
	lines := strings.Split(e.Message, "\n")
	if len(lines) < 2 {
		return
	}
	s.ProcessMembersList(lines[1], false)
	s.Active = true
	s.TimerManager.StopTimer(JOIN_TIMER_ID)
	s.StartAllTimers()
	printMembershipTable(s)
	log.Info("Node join completed.")
}

// Start the node process and launch all the threads
func startNode(s *server.Server) {
	log.Infof("Node %s is starting...\n", s.Self.ID)

	go receiverRoutine(s) // to receive requests from network
	go senderRoutine(s)   // to send gossip messages
	go inputRoutine(s)    // to receive requests from stdin

	// Blocks until either new message received or timer signals timeout
	for {
		select {
		case e := <-s.TimerManager.TimeoutChannel:
			handleTimeout(s, e)
		case e := <-s.ReceiverChannel:
			handleRequest(s, e)
		case e := <-s.InputChannel:
			handleCommand(s, e)
		}
	}
}

// Function to handle the Join request by new node at any node
func handleJoinRequest(s *server.Server, e server.ReceiverEvent) {
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
		log.Errorf("Failed to add host: %s\n", err.Error())
		reply := fmt.Sprintf("%s\n%s\n", JOIN_ERROR, err.Error())
		s.Connection.WriteToUDP([]byte(reply), e.Sender)
		return
	}

	reply := fmt.Sprintf("%s\n%s\n", JOIN_OK, s.EncodeMembersList())
	_, err = s.Connection.WriteToUDP([]byte(reply), host.Address)
	if err != nil {
		log.Error(err)
	}

	printMembershipTable(s)
}

// Introducer process accepts new hosts and sends full membership list
func loadKnownHosts(s *server.Server) {
	log.Info("Loading known hosts...")
	save_file, err := os.OpenFile(server.SAVE_FILENAME, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("Failed to open file: %s\n", err.Error())
	}
	defer save_file.Close()
	scanner := bufio.NewScanner(save_file)
	if scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) > 0 {
			s.ProcessMembersList(line, false)
		}
	}

	log.Infof("Added %d hosts: %s\n", len(s.Members), s.EncodeMembersList())
	s.StartAllTimers()
}
