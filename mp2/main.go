package main

import (
	"cs425/server"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Introducer struct {
	Hosts     []server.Host
	HostsLock sync.Mutex
}

func (self *Introducer) AddHost(host server.Host) {
	self.HostsLock.Lock()
	defer self.HostsLock.Unlock()

	// Check if the host already exists
	for i, h := range self.Hosts {
		if h.Address == host.Address && h.Port == host.Port {
			// Update the ID if it's a duplicate
			self.Hosts[i].ID = host.ID
			return
		}
	}

	// Add the new host
	self.Hosts = append(self.Hosts, host)
}

func (self *Introducer) Handle(s *server.Server, peer *net.UDPAddr, message string) {
	tokens := strings.Split(message, ":")

	log.Println(message)

	if len(tokens) < 3 {
		return
	}

	self.AddHost(server.NewHost(tokens[0], tokens[1], tokens[2]))
	self.HostsLock.Lock()
	defer self.HostsLock.Unlock()

	var hostStrings []string
	for _, host := range self.Hosts {
		hostStrings = append(hostStrings, host.GetSignature())
	}
	var response = strings.Join(hostStrings, ";") + "\n"
	s.Connection.WriteToUDP([]byte(response), peer)
}

func main() {
	if len(os.Args) < 2 {
		program := filepath.Base(os.Args[0])
		log.Fatalf("Usage: %s <port>", program)
	}

	var err error

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	s, err := server.NewServer(port)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Server listening on port %d\n", port)

	defer s.Close()
	s.Listen(&Introducer{})

}
