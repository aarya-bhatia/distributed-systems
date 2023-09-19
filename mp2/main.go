package main

import (
	"bufio"
	"cs425/server"
	"cs425/timer"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	NODES_PER_ROUND       = 2 // Number of random peers to send gossip every round
	INTRODUCER_ID         = "1"
	INTRODUCER_HOST       = "127.0.0.1" // TODO: Update this with VM1 in prod
	INTRODUCER_PORT       = 6001
	JOIN_OK               = "JOIN_OK"
	JOIN_ERROR            = "JOIN_ERROR"
	ERROR_ILLEGAL_REQUEST = JOIN_ERROR + "\n" + "Illegal Request" + "\n"
	JOIN_RETRY_TIMEOUT    = time.Second * 5
)

// Starts a UDP server on specified port
func main() {
	if len(os.Args) < 4 {
		program := filepath.Base(os.Args[0])
		log.Fatalf("Usage: %s <hostname> <port> <id>", program)
	}
	var port int
	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

	id := os.Args[3]
	if id == INTRODUCER_ID && port != INTRODUCER_PORT {
		log.Fatalf("Introducer port should be %d", INTRODUCER_PORT)
	}

	hostname := os.Args[1]
	s, err := server.NewServer(hostname, port, id)
	if err != nil {
		log.Fatal(err)
	}

	if os.Getenv("DEBUG") != "TRUE" {
		logfile := fmt.Sprintf("%s.log", s.Self.Signature)
		f, err := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		log.SetOutput(f)
		os.Stderr.WriteString(fmt.Sprintf("Log File: %s\n", logfile))
	}

	log.Printf("Server %s listening on port %d\n", id, port)
	defer s.Close()

	if id == INTRODUCER_ID {
		s.Introducer = true
		loadKnownHosts(s)
		startNode(s)
		return
	}

	err = joinWithRetry(s)
	if err != nil {
		log.Fatal(err)
	}

	startNode(s)
}

// Sends membership list to random subset of peers every T_gossip period
// Updates own counter and timestamp before sending the membership list
func sendPings(s *server.Server) {
	targets := selectRandomTargets(s, NODES_PER_ROUND)
	if len(targets) == 0 {
		return
	}

	log.Printf("Sending gossip to %d hosts", len(targets))

	s.MemberLock.Lock()
	s.Self.Counter++
	s.Self.UpdatedAt = time.Now().UnixMilli()
	s.MemberLock.Unlock()

	message := s.GetPingMessage()

	for _, target := range targets {
		n, err := s.Connection.WriteToUDP([]byte(message), target.Address)
		if err != nil {
			log.Println(err)
			continue
		}
		s.TotalByte += n
		log.Printf("Sent %d bytes to %s\n", n, target.Signature)
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

// Request introducer to join node and receive initial membership list
// If introducer is down, it will retry the request every JOIN_RETRY_TIMEOUT period.
func joinWithRetry(s *server.Server) error {
	request := s.GetJoinMessage()
	messageChannel := make(chan string, 1)

	go func() {
		for {
			message, _, err := s.GetPacket()

			if err != nil {
				log.Fatalf("Error reading packet: %s", err.Error())
			}

			if strings.Index(message, JOIN_OK) == 0 {
				messageChannel <- message
				break
			}

			if strings.Index(message, JOIN_ERROR) == 0 {
				log.Fatalf("Failed to join: %s", message)
			}
		}
	}()

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", INTRODUCER_HOST, INTRODUCER_PORT))
	if err != nil {
		log.Fatal(err)
	}

	for {
		s.Connection.WriteToUDP([]byte(request), addr)

		if err != nil {
			return err
		}

		select {
		case messageReply := <-messageChannel:
			lines := strings.Split(messageReply, "\n")
			if len(lines) < 2 {
				continue
			}
			s.ProcessMembersList(lines[1])
			log.Println("Node join completed.")
			return nil
		case <-time.After(JOIN_RETRY_TIMEOUT):
			fmt.Println("Timeout: Retrying join...")
		}
	}
}

// Timeout signal received from timer
// Either suspect node or mark failed
// Updates the known_hosts file for introducer
func handleTimeout(s *server.Server, e timer.TimerEvent) {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	if host, ok := s.Members[e.ID]; ok {
		if host.Suspected || s.SuspicionTimeout == 0 {
			log.Printf("FAILURE DETECTED: Node %s is considered failed\n", e.ID)
			delete(s.Members, e.ID)
			if s.Introducer {
				s.MemberLock.Unlock()
				s.SaveMembersToFile()
				s.MemberLock.Lock()
			}
		} else {
			log.Printf("FAILURE SUSPECTED: Node %s is suspected of failure\n", e.ID)
			host.Suspected = true
			s.TimerManager.RestartTimer(e.ID, s.SuspicionTimeout)
		}
	}
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
				log.Println("Starting gossip...")
			} else {
				log.Println("Stopping gossip...")
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
			log.Println(err)
			continue
		}

		s.ReceiverChannel <- server.ReceiverEvent{Message: message, Sender: sender}

	}
}

// Handles the request received by the server
// JOIN, PING, ID, LIST, KILL, START_GOSSIP, STOP_GOSSIP, CONFIG
func handleRequest(s *server.Server, e server.ReceiverEvent) {
	log.Println("Request received: ", e)

	lines := strings.Split(e.Message, "\n")
	if len(lines) < 1 {
		return
	}

	header := lines[0]
	verbs := strings.Split(header, " ")

	switch verb := verbs[0]; verb {
	case "JOIN":
		handleJoinRequest(s, e)

	case "PING":
		if rand.Intn(100) < s.DropRate {
			log.Printf("PING from %s dropped with drop rate %d %%\n", e, s.DropRate)
			return
		}
		if len(lines) < 2 {
			return
		}
		if s.Active {
			s.ProcessMembersList(lines[1])
		}

	case "ID":
		s.Connection.WriteToUDP([]byte(fmt.Sprintf("%s\n", s.Self.ID)), e.Sender)

	case "LIST":
		s.Connection.WriteToUDP([]byte(fmt.Sprintf("OK\n%s\n", s.EncodeMembersList())), e.Sender)

	case "KILL":
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)

	case "START_GOSSIP":
		s.Active = true
		s.GossipChannel <- true
		err := joinWithRetry(s)
		if err != nil {
			log.Fatal(err)
		}

	case "STOP_GOSSIP":
		s.Active = false
		s.GossipChannel <- false
		s.MemberLock.Lock()
		for ID := range s.Members {
			delete(s.Members, ID)
		}
		s.MemberLock.Unlock()
		s.TimerManager.StopAll()

	case "CONFIG":
		handleConfigRequest(s, e)

	default:
		log.Println("WARNING: Unknown request verb: ", verb)
	}
}

// Start the node process and launch all the threads
func startNode(s *server.Server) {
	log.Printf("Node %s is starting...\n", s.Self.ID)
	go receiverRoutine(s)
	go senderRoutine(s)

	// Blocks until either new message received or timer signals timeout
	for {
		select {
		case e := <-s.TimerManager.TimeoutChannel:
			handleTimeout(s, e)
		case e := <-s.ReceiverChannel:
			handleRequest(s, e)
		}
	}
}

// Function to handle the Join request by new node at Introducer
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
		log.Printf("Failed to add host: %s\n", err.Error())
		reply := fmt.Sprintf("%s\n%s\n", JOIN_ERROR, err.Error())
		s.Connection.WriteToUDP([]byte(reply), e.Sender)
		return
	}

	reply := fmt.Sprintf("%s\n%s\n", JOIN_OK, s.EncodeMembersList())
	_, err = s.Connection.WriteToUDP([]byte(reply), host.Address)
	if err != nil {
		log.Println(err)
	}
}

// Introducer process accepts new hosts and sends full membership list
func loadKnownHosts(s *server.Server) {
	log.Println("Loading known hosts...")
	save_file, err := os.OpenFile(server.SAVE_FILENAME, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("Failed to open file: %s\n", err.Error())
	}
	defer save_file.Close()
	scanner := bufio.NewScanner(save_file)
	if scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) > 0 {
			s.ProcessMembersList(line)
		}
	}

	log.Printf("Added %d hosts: %s\n", len(s.Members), s.EncodeMembersList())
}
