package main

import (
	"cs425/server"
	"cs425/timer"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const NODES_PER_ROUND = 2 // Number of random peers to send gossip every round

// Sends membership list to random subset of peers every T_gossip period
func RandomGossipRoutine(s *server.Server) {
	for {
		message := s.GetPingMessage()
		targets := SelectRandomTargets(s, NODES_PER_ROUND)
		if len(targets) > 0 {
			log.Printf("Sending gossip to %d hosts", len(targets))
			s.MemberLock.Lock()
			s.Self.Counter++
			s.Self.UpdatedAt = time.Now().UnixMilli()
			s.MemberLock.Unlock()
			for _, target := range targets {
				// Determin if we should drop a message
				if rand.Intn(100) < s.DropRate {
					log.Printf("Message to %s dropped with drop rate %d%\n", target.Address, s.DropRate)
					continue
				}
				n, err := s.Connection.WriteToUDP([]byte(message), target.Address)
				if err != nil {
					log.Println(err)
					continue
				}
				s.TotalByte += n
				log.Printf("Sent %d bytes to %s\n", n, target.Signature)
			}
		}
		time.Sleep(timer.T_GOSSIP)
	}
}

// Selects at most 'count' number of hosts from list
func SelectRandomTargets(s *server.Server, count int) []*server.Host {
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

// Request introducer to join node
func JoinWithRetry(s *server.Server) error {
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

			// If PING request, that's also OK
		}
	}()

	for {
		_, err := s.SendPacket(INTRODUCER_HOST, INTRODUCER_PORT, []byte(request))
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
		case <-time.After(timer.T_GOSSIP):
			fmt.Println("Timeout: Retrying join...")
		}
	}
}

func handleMessage(s *server.Server, message string) {
	if strings.Index(message, "PING") == 0 {
		lines := strings.Split(message, "\n")
		if len(lines) < 2 {
			return
		}
		s.ProcessMembersList(lines[1])
	} else if strings.Index(message, "JOIN") == 0 {
		HandleJoin(s, message)
	} else if strings.Index(message, "CONFIG") == 0 {
		handleConfig(s, message)
	}
}

func handleTimeout(s *server.Server, e timer.TimerEvent) {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	if host, ok := s.Members[e.ID]; ok {
		if host.Suspected || timer.T_CLEANUP == 0 {
			log.Printf("FAILURE DETECTED: Node %s is considered failed\n", e.ID)
			delete(s.Members, e.ID)
			s.TimerManager.RestartTimer(e.ID, timer.T_CLEANUP)
		} else {
			log.Printf("FAILURE SUSPECTED: Node %s is suspected of failure\n", e.ID)
			host.Suspected = true
			s.TimerManager.RestartTimer(e.ID, timer.T_CLEANUP)
		}
	}
}

// Handle netcat config command using protocal CONFIG <field to change> <value>
func handleConfig(s *server.Server, message string) {
	words := strings.Split(message, " ")
	if len(words) < 3 {
		return
	}
	if words[1] == "droprate" {
		dropRate, err := strconv.Atoi(words[2])
		if err != nil {
			return
		}
		s.DropRate = dropRate
	}
}

// Start FD node process
func StartNode(s *server.Server) {
	go RandomGossipRoutine(s)

	messageReceivedChannel := make(chan string)
	go func() {
		for {
			message, client, err := s.GetPacket()
			if err != nil {
				log.Println(err)
				continue
			}

			if strings.Index(message, "ID") == 0 {
				s.Connection.WriteToUDP([]byte(fmt.Sprintf("%s\n", s.Self.ID)), client)
			} else if strings.Index(message, "LIST") == 0 {
				s.Connection.WriteToUDP([]byte(fmt.Sprintf("OK\n%s\n", s.EncodeMembersList())), client)
			} else if strings.Index(message, "KILL") == 0 {
				syscall.Kill(syscall.Getpid(), syscall.SIGINT)
			} else {
				messageReceivedChannel <- message
			}
		}
	}()

	for {
		select {
		case message := <-messageReceivedChannel:
			handleMessage(s, message)
		case timerEvent := <-s.TimerManager.TimeoutChannel:
			handleTimeout(s, timerEvent)
		}
	}
}

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

	f, err := os.OpenFile(fmt.Sprintf("%s.log", s.Self.Signature), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	log.SetOutput(f)

	log.Printf("Server %s listening on port %d\n", id, port)
	defer s.Close()

	if id == INTRODUCER_ID {
		s.Introducer = true
		LoadKnownHosts(s)
		log.Println("Introducer is online...")
		StartNode(s)
	} else {
		err := JoinWithRetry(s)
		if err != nil {
			log.Fatal(err)
		}
		StartNode(s)
	}
}
