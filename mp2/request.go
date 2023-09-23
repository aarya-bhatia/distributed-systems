package main

import (
	"cs425/server"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"strconv"
	"strings"
)

func HandleCommand(s *server.Server, command string) {
	commands := []string{"list_mem: print membership table", "list_self: print id of node",
		"kill: crash server", "join: start gossiping", "leave: stop gossiping", "sus_on: enable gossip suspicion",
		"sus_off: disable gossip suspicion", "help: list all commands"}

	switch strings.ToLower(command) {

	case "ls":
		fallthrough
	case "list_mem":
		s.PrintMembershipTable()

	case "id":
		fallthrough
	case "list_self":
		fmt.Println(s.Self.ID)

	case "kill":
		log.Fatalf("Kill command received!")

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

	case "sus_on":
		s.Protocol = server.GOSPSIP_SUSPICION_PROTOCOL
		fmt.Println("OK")

	case "sus_off":
		s.Protocol = server.GOSSIP_PROTOCOL
		fmt.Println("OK")

	case "help":
		for i := range commands {
			fmt.Printf("%d. %s\n", i+1, commands[i])
		}
	}
}

// Handles the request received by the server
// JOIN, PING, ID, LIST, KILL, START_GOSSIP, STOP_GOSSIP, CONFIG, SUS ON, SUS OFF, LIST_SUS
func HandleRequest(s *server.Server, e server.ReceiverEvent) {
	lines := strings.Split(e.Message, "\n")
	if len(lines) < 1 {
		return
	}

	header := lines[0]
	tokens := strings.Split(header, " ")

	log.Debugf("Request %s received from: %v\n", tokens[0], e.Sender)

	switch verb := strings.ToLower(tokens[0]); verb {
	case "join":
		HandleJoinRequest(s, e)

	case "join_ok":
		HandleJoinResponse(s, e)

	case "join_error":
		log.Warnf("Failed to join: %s", e.Message)

	case "ping":
		HandlePingRequest(s, e)

	case "id":
		s.Connection.WriteToUDP([]byte(fmt.Sprintf("%s\n", s.Self.ID)), e.Sender)

	case "ls":
		s.Connection.WriteToUDP([]byte(fmt.Sprintf("OK\n%s\n", strings.ReplaceAll(s.EncodeMembersList(false), ";", "\n"))), e.Sender)

	case "kill":
		log.Fatalf("Kill request received\n")

	case "start_gossip":
		startGossip(s)

	case "stop_gossip":
		stopGossip(s)

	case "config":
		HandleConfigRequest(s, e)

	case "sus":
		HandleSusRequest(s, e)

	case "list_sus":
		HandleListSus(s, e)

	default:
		log.Warn("Unknown request verb: ", verb)
	}
}

func HandleJoinResponse(s *server.Server, e server.ReceiverEvent) {
	lines := strings.Split(e.Message, "\n")
	if len(lines) < 2 || (s.Active && !IsIntroducer(s)) {
		return
	}

	log.Info("Join accepted by ", e.Sender)

	s.TimerManager.StopTimer(JOIN_TIMER_ID)
	s.ProcessMembersList(lines[1])
	// s.StartAllTimers()
	s.Active = true
	log.Info("Node join completed.")
}

// Handle config command: CONFIG <field to change> <value>
func HandleConfigRequest(s *server.Server, e server.ReceiverEvent) {
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

func HandlePingRequest(s *server.Server, e server.ReceiverEvent) {
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

	s.ProcessMembersList(lines[1])
}

func HandleListSus(s *server.Server, e server.ReceiverEvent) {
	s.MemberLock.Lock()
	defer s.MemberLock.Unlock()

	arr := []string{}

	for _, host := range s.Members {
		if host.Suspected {
			arr = append(arr, host.Signature)
		}
	}

	reply := fmt.Sprintf("OK\n%s\n", strings.Join(arr, "\n"))
	s.Connection.WriteToUDP([]byte(reply), e.Sender)
}

func HandleSusRequest(s *server.Server, e server.ReceiverEvent) {
	lines := strings.Split(e.Message, "\n")
	tokens := strings.Split(lines[0], " ")
	if len(tokens) < 2 {
		return
	}
	if strings.ToUpper(tokens[1]) == "ON" {
		s.Protocol = server.GOSPSIP_SUSPICION_PROTOCOL
	} else if strings.ToUpper(tokens[1]) == "OFF" {
		s.Protocol = server.GOSSIP_PROTOCOL
	}
}

// Function to handle the Join request by new node at any node
func HandleJoinRequest(s *server.Server, e server.ReceiverEvent) {
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
		log.Errorf("Failed to add host: %s\n", err.Error())
		reply := fmt.Sprintf("%s\n%s\n", JOIN_ERROR, err.Error())
		s.Connection.WriteToUDP([]byte(reply), e.Sender)
		return
	}

	reply := fmt.Sprintf("%s\n%s\n", JOIN_OK, s.EncodeMembersList(true))
	_, err = s.Connection.WriteToUDP([]byte(reply), host.Address)
	if err != nil {
		log.Error(err)
	}
}
