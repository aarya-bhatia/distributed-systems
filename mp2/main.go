package main

import (
	"bufio"
	"cs425/server"
	"cs425/timer"
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

const JOIN_RETRY_TIMEOUT = time.Second * 10
const JOIN_OK = "JOIN_OK"
const JOIN_ERROR = "JOIN_ERROR"
const JOIN_TIMER_ID = "JOIN_TIMER"
const DEFAULT_PORT = 6000
const NODES_PER_ROUND = 2 // Number of random peers to send gossip every round
const ERROR_ILLEGAL_REQUEST = JOIN_ERROR + "\n" + "Illegal Request" + "\n"

type Node struct {
	Hostname string
	Port     int
}

var cluster = []Node{
	{"fa23-cs425-0701.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0702.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0703.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0704.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0705.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0706.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0707.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0708.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0709.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0710.cs.illinois.edu", DEFAULT_PORT},
}

var local_cluster = []Node{
	{"localhost", 6001},
	{"localhost", 6002},
	{"localhost", 6003},
	{"localhost", 6004},
	{"localhost", 6005},
}

// Starts a UDP server on specified port
func main() {
	var err error
	var port int
	var hostname string
	var level string
	var env string
	var withSuspicion bool

	systemHostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	flag.IntVar(&port, "p", DEFAULT_PORT, "server port number")
	flag.StringVar(&hostname, "h", systemHostname, "server hostname")
	flag.StringVar(&level, "l", "DEBUG", "log level")
	flag.StringVar(&env, "e", "production", "environment: development, production")
	flag.BoolVar(&withSuspicion, "s", false, "gossip with suspicion")
	flag.Parse()

	if hostname == "localhost" || hostname == "127.0.0.1" {
		env = "development"
	}

	if env == "development" {
		cluster = local_cluster
		hostname = "localhost"
		log.Info("Using local cluster")
	}

	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})

	log.SetReportCaller(false)
	log.SetOutput(os.Stderr)

	switch strings.ToLower(level) {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	}

	log.Debug(cluster)

	s, err := server.NewServer(hostname, port)
	if err != nil {
		log.Fatal(err)
	}

	if withSuspicion {
		s.SuspicionTimeout = server.T_CLEANUP
	}

	if IsIntroducer(s) {
		s.Active = true
		log.Info("Introducer is online...")
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
	startNode(s)
}

// Fix the first node as the introducer
func IsIntroducer(s *server.Server) bool {
	return s.Self.Hostname == cluster[0].Hostname && s.Self.Port == cluster[0].Port
}

// Start the node process and launch all the threads
func startNode(s *server.Server) {
	log.Infof("Node %s is starting...\n", s.Self.ID)

	go receiverRoutine(s) // to receive requests from network
	go senderRoutine(s)   // to send gossip messages
	go inputRoutine(s)    // to receive requests from stdin

	sendJoinRequest(s)

	// Blocks until either new message received or timer sends a signal
	for {
		select {
		case e := <-s.TimerManager.TimeoutChannel:
			HandleTimeout(s, e)
		case e := <-s.ReceiverChannel:
			HandleRequest(s, e)
		case e := <-s.InputChannel:
			HandleCommand(s, e)
		}
	}
}


// Sends membership list to random subset of peers every T_gossip period
// Updates own counter and timestamp before sending the membership list
func sendPings(s *server.Server) {
	targets := selectRandomTargets(s, NODES_PER_ROUND)
	if len(targets) == 0 {
		return
	}

	log.Debugf("Sending gossip to %d hosts", len(targets))

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
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	var hosts = []*server.Host{}
	for _, host := range s.Members {
		if host.ID != s.Self.ID && !host.Suspected {
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
func HandleTimeout(s *server.Server, e timer.TimerEvent) {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	if e.ID == s.Self.ID {
		return
	}

	if e.ID == JOIN_TIMER_ID {
		if IsIntroducer(s) {
			if len(s.Members) <= 1 {
				log.Info("Timeout: Retrying JOIN.")
				sendJoinRequest(s)
			}
		} else if !s.Active {
			log.Info("Timeout: Retrying JOIN.")
			sendJoinRequest(s)
		}

		return
	}

	if host, ok := s.Members[e.ID]; ok {
		if host.Suspected || s.SuspicionTimeout == 0 {
			log.Warnf("FAILURE DETECTED: Node %s is considered failed\n", e.ID)
			delete(s.Members, e.ID)
			s.MemberLock.Unlock()
			// sendNotification(s, fmt.Sprintf("FAILURE %s\n", e.ID))
			s.MemberLock.Lock()
		} else {
			log.Warnf("FAILURE SUSPECTED: Node %s is suspected of failure\n", e.ID)
			host.Suspected = true
			s.TimerManager.RestartTimer(e.ID, s.SuspicionTimeout)
		}

		s.MemberLock.Unlock()
		s.PrintMembershipTable()
		s.MemberLock.Lock()

		return
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
			// ghostEntryRemover(s)
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
	if IsIntroducer(s) {
		log.Warn("Introducer is always active")
		return
	}

	if s.Active {
		log.Warn("server is already active")
		return
	}

	ID := s.SetUniqueID()
	log.Debugf("Updated Node ID to %s", ID)
	s.GossipChannel <- true
	sendJoinRequest(s)
}

func stopGossip(s *server.Server) {
	if IsIntroducer(s) {
		log.Warn("Introducer is always active")
		return
	}

	if !s.Active {
		log.Warn("server is already inactive")
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

// Send request to join node and start timer
func sendJoinRequest(s *server.Server) {
	msg := s.GetJoinMessage()

	if IsIntroducer(s) {
		for _, vm := range cluster {
			if vm.Hostname == s.Self.Hostname && vm.Port == s.Self.Port {
				continue
			}

			addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", vm.Hostname, vm.Port))
			if err != nil {
				log.Fatal(err)
			}

			_, err = s.Connection.WriteToUDP([]byte(msg), addr)
			if err != nil {
				log.Println(err)
				continue
			}

			log.Printf("Sent join request to %s:%d\n", vm.Hostname, vm.Port)
		}

	} else {

		introducer := cluster[0]

		addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", introducer.Hostname, introducer.Port))
		if err != nil {
			log.Fatal(err)
		}

		s.Connection.WriteToUDP([]byte(msg), addr)
		log.Printf("Sent join request to %s:%d\n", introducer.Hostname, introducer.Port)
	}

	s.TimerManager.RestartTimer(JOIN_TIMER_ID, JOIN_RETRY_TIMEOUT)
}
