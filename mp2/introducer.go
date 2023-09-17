package main

import (
	"bufio"
	"cs425/server"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	SAVE_FILENAME   = "known_hosts"
	INTRODUCER_ID   = "1"
	INTRODUCER_HOST = "127.0.0.1" // TODO: Update this with VM1 in prod
	INTRODUCER_PORT = 6001
	JOIN_OK         = "JOIN_OK"
	JOIN_ERROR      = "JOIN_ERROR"
)

type IllegalMessage struct {
	message string
}

func (_ *IllegalMessage) Error() string {
	return "Illegal Message"
}

func HandleJoin(s *server.Server, message string) error {
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
			reply := fmt.Sprintf("%s\n%s\n", JOIN_ERROR, err.Error())
			_, err = s.SendPacket(senderAddress, senderPortInt, []byte(reply))
			return err
		} else {
			if s.Introducer {
				SaveMembersToFile(s)
			}

			log.Printf("New host added: %s\n", host.Signature)
			reply := fmt.Sprintf("%s\n%s\n", JOIN_OK, s.EncodeMembersList())
			_, err = s.SendPacket(senderAddress, senderPortInt, []byte(reply))
			return err
		}
	}

	return nil
}

func SaveMembersToFile(s *server.Server) {
	save_file, err := os.OpenFile(SAVE_FILENAME, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("Failed to open file: %s\n", err.Error())
	}
	defer save_file.Close()
	save_file.WriteString(s.EncodeMembersList() + "\n")
}

// Introducer process accepts new hosts and sends full membership list
func LoadKnownHosts(s *server.Server) {
	save_file, err := os.OpenFile(SAVE_FILENAME, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("Failed to open file: %s\n", err.Error())
	}
	defer save_file.Close()
	scanner := bufio.NewScanner(save_file)
	log.Println("Loading hosts from file...")
	if scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) > 0 {
			s.ProcessMembersList(line)
		}
	}

	log.Printf("Added %d hosts: %s\n", len(s.Members), s.EncodeMembersList())
}
