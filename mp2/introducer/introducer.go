package introducer

import (
	"cs425/server"
	"fmt"
	"log"
	"strconv"
	"strings"
)

const INTRODUCER_ID = "1"
const INTRODUCER_HOST = "127.0.0.1" // TODO: Update this with VM1 in prod
const INTRODUCER_PORT = 6001

const JOIN_OK = "JOIN_OK"
const JOIN_ERROR = "JOIN_ERROR"

type IllegalMessage struct {
	message string
}

func (_ *IllegalMessage) Error() string {
	return "Illegal Message"
}

func handleMessage(s *server.Server, message string) error {
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
			return s.SendPacket(senderAddress, senderPortInt, []byte(reply))
		} else {
			log.Printf("New host added: %s\n", host.GetSignature())
			reply := fmt.Sprintf("%s\n%s\n", JOIN_OK, s.EncodeMembersList())
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

		err = handleMessage(s, message)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}
