package introducer

import (
	"bufio"
	"cs425/server"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const SAVE_FILENAME = "known_hosts"

var save_file *os.File = nil

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

		if save_file != nil {
			save_file.WriteString(senderInfo)
			if senderInfo[len(senderInfo)-1] != '\n' {
				save_file.WriteString("\n")
			}
		}

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
	var err error = nil
	if save_file == nil {
		save_file, err = os.OpenFile(SAVE_FILENAME, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	}
	if err != nil {
		log.Fatalf("Failed to open file: %s\n", err.Error())
	}

	defer save_file.Close()

	scanner := bufio.NewScanner(save_file)

	log.Println("Loading hosts from file...")
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 {
			continue
		}

		tokens := strings.Split(line, ":")
		if len(tokens) < 3 {
			continue
		}

		port, err := strconv.Atoi(tokens[1])
		if err != nil {
			log.Fatal(err)
		}

		s.AddHost(tokens[0], port, tokens[2])
	}
	log.Printf("Added %d hosts\n", len(s.Members))

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

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
