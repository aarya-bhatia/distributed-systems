package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

type BlockMetadata struct {
	Filename         string
	Version          int
	BlockID          int
	BlockOffset      int
	BlockSize        int
	PrimaryReplicaID int
	BackupReplicaIDs []int
}

type BlockData struct {
	Filename    string
	Version     int
	BlockID     int
	BlockOffset int
	BlockSize   int
	Data        []byte
}

type File struct {
	Filename  string
	CreatorID int
	Version   int
	FileSize  int
	Blocks    []BlockMetadata
}

type Request struct {
	RequestType int
	Action      int
	Filename    string
	ClientID    int
}

type NodeInfo struct {
	ID       int
	Hostname string
	Port     int
	State    int
}

const (
	ENV            = "DEV"
	REQUEST_READ   = 0
	REQUEST_WRITE  = 1
	STATE_ALIVE    = 0
	STATE_FAILED   = 1
	DEFAULT_PORT   = 5000
	REPLICA_FACTOR = 2
	MAX_BLOCK_SIZE = 4096 * 1024 // 4 MB
)

var nodes []*NodeInfo = []*NodeInfo{
	{ID: 1, Hostname: "localhost", Port: 5000, State: STATE_ALIVE},
	{ID: 2, Hostname: "localhost", Port: 5001, State: STATE_ALIVE},
	{ID: 3, Hostname: "localhost", Port: 5002, State: STATE_ALIVE},
	{ID: 4, Hostname: "localhost", Port: 5003, State: STATE_ALIVE},
}

var serverFiles map[string]*File
var serverQueue []*Request
var serverBlocks map[string]*BlockData

// To handle replicas after a node fails or rejoins
func rebalance() {
	m := map[int]bool{}
	for _, node := range nodes {
		m[node.ID] = node.State == STATE_ALIVE
	}
	// Get affected replicas
	// Add tasks to replicate the affected blocks to queue
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: ./main <tcp_port> <udp_port>")
		return
	}

	tcpPort := os.Args[1]
	udpPort := os.Args[2]

	tcpPortInt, err := strconv.Atoi(tcpPort)
	if err != nil {
		log.Fatal(err)
	}

	udpPortInt, err := strconv.Atoi(udpPort)
	if err != nil {
		log.Fatal(err)
	}

	c := make(chan bool)

	go StartTCPServer(tcpPortInt)
	go StartUDPServer(udpPortInt)

	<-c
}
