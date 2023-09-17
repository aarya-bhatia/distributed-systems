package main

import (
	"cs425/introducer"
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

const NODES_PER_ROUND = 2          // Number of random peers to send gossip every round
const T_GOSSIP = 5 * time.Second   // Time duration between each gossip round
const T_TIMEOUT = 10 * time.Second // Time duration until a peer times out
var T_CLEANUP = 5 * time.Second    // Time duration before peer is deleted

var timerManager = timer.NewTimerManager()

// Sends membership list to random subset of peers every T_gossip period
func RandomGossipRoutine(s *server.Server) {
	for {
		message := s.GetPingMessage()
		targets := SelectRandomTargets(s, NODES_PER_ROUND)
		log.Printf("Sending gossip to %d hosts: %s", len(targets), message)
		s.MemberLock.Lock()
		s.Members[s.ID].Counter++
		s.Members[s.ID].UpdatedAt = time.Now().UnixMilli()
		s.MemberLock.Unlock()
		for _, target := range targets {
			n, err := s.Connection.WriteToUDP([]byte(message), target.UDPAddr)
			if err != nil {
				log.Println(err)
				continue
			}
			log.Printf("Sent %d bytes to %s\n", n, target.GetSignature())
		}
		time.Sleep(T_GOSSIP)
	}
}

// Selects at most 'count' number of hosts from list
func SelectRandomTargets(s *server.Server, count int) []*server.Host {
	var hosts = []*server.Host{}
	s.MemberLock.Lock()
	for _, host := range s.Members {
		if host.ID != s.ID {
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

			if strings.Index(message, introducer.JOIN_OK) == 0 {
				messageChannel <- message
				break
			}

			if strings.Index(message, introducer.JOIN_ERROR) == 0 {
				log.Fatalf("Failed to join: %s", message)
			}
		}
	}()

	for {
		_, err := s.SendPacket(introducer.INTRODUCER_HOST, introducer.INTRODUCER_PORT, []byte(request))
		if err != nil {
			return err
		}

		select {
		case messageReply := <-messageChannel:
			lines := strings.Split(messageReply, "\n")
			if len(lines) < 2 {
				log.Fatalf("Illegal Join reply: %s", messageReply)
			}

			initialMembers := strings.Split(lines[1], ";")

			for _, member := range initialMembers {
				tokens := strings.Split(member, ":")
				if len(tokens) < 3 {
					continue
				}

				port, err := strconv.Atoi(tokens[1])
				if err != nil {
					log.Println(err)
					continue
				}

				address := tokens[0]
				id := tokens[2]

				s.AddHost(address, port, id)
			}

			log.Println("Node join completed!")
			return nil

		case <-time.After(5 * time.Second):
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

		members := strings.Split(lines[1], ";")
		timeNow := time.Now().UnixMilli()

		s.MemberLock.Lock()
		for _, member := range members {
			tokens := strings.Split(member, ":")

			if len(tokens) < 4 {
				return
			}

			address, port, id, counter := tokens[0], tokens[1], tokens[2], tokens[3]
			portInt, err := strconv.Atoi(port)
			if err != nil {
				log.Println(err)
				return
			}

			counterInt, err := strconv.Atoi(counter)
			if err != nil {
				log.Println(err)
				return
			}

			found, ok := s.Members[id]
			if !ok {
				s.MemberLock.Unlock()
				found, err = s.AddHost(address, portInt, id)
				if err != nil {
					log.Println(err)
					continue
				}
				s.MemberLock.Lock()
				s.Members[id].Counter = counterInt
				found.UpdatedAt = timeNow
				s.Members[id].Suspected = false
				timerManager.RestartTimer(id, T_TIMEOUT)
			} else if found.Counter < counterInt {
				found.Counter = counterInt
				found.UpdatedAt = timeNow
				log.Printf("Updated counter: %s\n", s.Members[id].GetSignatureWithCount())
				s.Members[id].Suspected = false
				timerManager.RestartTimer(id, T_TIMEOUT)
			}
		}
		s.MemberLock.Unlock()
	}
}

func handleTimeout(s *server.Server, e timer.TimerEvent) {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	if host, ok := s.Members[e.ID]; ok {
		if host.Suspected || T_CLEANUP == 0 {
			log.Printf("FAILURE DETECTED: Node %s is considered failed\n", e.ID)
			delete(s.Members, e.ID)
		} else {
			log.Printf("FAILURE SUSPECTED: Node %s is suspected of failure\n", e.ID)
			host.Suspected = true
			timerManager.RestartTimer(e.ID, T_CLEANUP)
		}
	}
}

// Start FD node process
func StartNode(s *server.Server) {
	err := JoinWithRetry(s)
	if err != nil {
		log.Fatal(err)
	}

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
				s.Connection.WriteToUDP([]byte(fmt.Sprintf("%s\n", s.ID)), client)
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
		case timerEvent := <-timerManager.TimeoutChannel:
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
	if id == introducer.INTRODUCER_ID && port != introducer.INTRODUCER_PORT {
		log.Fatalf("Introducer port should be %d", introducer.INTRODUCER_PORT)
	}

	hostname := os.Args[1]
	s, err := server.NewServer(hostname, port, id)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Server %s listening on port %d\n", id, port)
	defer s.Close()

	if id == introducer.INTRODUCER_ID {
		s.Introducer = true
		introducer.StartIntroducer(s)
	} else {
		StartNode(s)
	}
}
