package main

import (
	"cs425/server"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// fa23-cs425-0701.cs.illinois.edu
// fa23-cs425-0702.cs.illinois.edu
// fa23-cs425-0703.cs.illinois.edu
// fa23-cs425-0704.cs.illinois.edu
// fa23-cs425-0705.cs.illinois.edu
// fa23-cs425-0706.cs.illinois.edu
// fa23-cs425-0707.cs.illinois.edu
// fa23-cs425-0708.cs.illinois.edu
// fa23-cs425-0709.cs.illinois.edu
// fa23-cs425-0710.cs.illinois.edu

const INTRODUCER_ID = "1"
const INTRODUCER_HOST = "127.0.0.1" // TODO: Update this with VM1 in prod
const INTRODUCER_PORT = 6001

const NODES_PER_ROUND = 2 // Number of random peers to send gossip every round

var T_GOSSIP = 1000  // Time duration in milliseconds between each gossip round
var T_TIMEOUT = 1000 // Time duration until a peer times out
var T_CLEANUP = 1000 // Time duration before peer is deleted

type IllegalMessage struct {
	messgae string
}

func (_ *IllegalMessage) Error() string {
	return "Illegal Message"
}

func HandleMessage(s *server.Server, message string) error {
	lines := strings.Split(message, "\n")
	if len(lines) < 1 {
		return &IllegalMessage{message}
	}

	tokens := strings.Split(lines[0], " ")
	if len(tokens) < 2 {
		return &IllegalMessage{message}
	}

	messageType, senderInfo := tokens[0], tokens[1]
	tokens = strings.Split(senderInfo, ":")
	if len(tokens) < 3 {
		return &IllegalMessage{message}
	}

	senderAddress, senderPort, senderId := tokens[0], tokens[1], tokens[2]
	senderPortInt, err := strconv.Atoi(senderPort)
	if err != nil {
		return err
	}

	if messageType == "JOIN" {
		host, err := s.AddHost(senderAddress, senderPortInt, senderId)
		if err != nil {
			log.Printf("Failed to add host: %s\n", err.Error())
			reply := fmt.Sprintf("JOIN_ERROR\n%s\n", err.Error())
			return s.SendPacket(senderAddress, senderPortInt, []byte(reply))
		} else {
			log.Printf("New host added: %s\n", host.GetSignature())
			reply := fmt.Sprintf("JOIN_OK\n%s\n", s.EncodeMembersList())
			return s.SendPacket(senderAddress, senderPortInt, []byte(reply))
		}
	}

	return nil
}

// Introducer process accepts new hosts and sends full membership list
func StartIntroducer(s *server.Server) {
	log.Println("Introducer is online...")

	for {
		message, _, err := s.GetPacket()
		if err != nil {
			log.Println(err)
			continue
		}

		err = HandleMessage(s, message)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}

// Update T_cleanup time
func UpdateSuspicionTimeout() {
}

// Sends membership list to random subset of peers every T_gossip period
func RandomGossipRoutine(s *server.Server) {
	for {

	}
}

// Selects 'count' number of hosts from list
func SelectRandomTargets(hosts []*server.Host, count int) []*server.Host {
	return hosts
}

// Request introducer to join node
func JoinWithRetry(s *server.Server) error {
	request := fmt.Sprintf("JOIN %s:%d:%s\n", s.HostName, s.Address.Port, s.ID)
	messageChannel := make(chan string, 1)

	go func() {
		for {
			message, _, err := s.GetPacket()
			// log.Printf("Recieved message: %s\n", message)

			if err != nil {
				log.Fatalf("Error reading packet: %s", err.Error())
			}

			if strings.Index(message, "JOIN_OK") == 0 {
				messageChannel <- message
				break
			}

			if strings.Index(message, "JOIN_ERROR") == 0 {
				log.Fatalf("Failed to join: %s", message)
			}
		}
	}()

	for {
		err := s.SendPacket(INTRODUCER_HOST, INTRODUCER_PORT, []byte(request))
		if err != nil {
			return err
		}

		select {
		case messageReply := <-messageChannel:
			lines := strings.Split(messageReply, "\n")
			if len(lines) < 2 {
				log.Fatalf("Illegal join reply: %s", messageReply)
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

				s.MemberLock.Lock()
				s.Members[id] = server.NewHost(address, port, id)
				log.Printf("Added new host: %s\n", s.Members[id].GetSignature())
				s.MemberLock.Unlock()
			}

			log.Println("Node join completed!")
			return nil

		case <-time.After(5 * time.Second):
			fmt.Println("Timeout: Retrying join...")
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
	// timerChannel := make(chan string)

	for {
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

	log.Printf("Server %s listening on port %d\n", id, port)
	defer s.Close()

	if id == INTRODUCER_ID {
		s.Introducer = true
		StartIntroducer(s)
	} else {
		s.Introducer = true
		StartNode(s)
	}
}
