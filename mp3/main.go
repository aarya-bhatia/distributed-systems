package main

import (
	"fmt"
	"net"
	"os"
	"strings"
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

// Returns slice of alive nodes
func GetAliveNodes(nodes []*NodeInfo) []*NodeInfo {
	res := []*NodeInfo{}
	for i, node := range nodes {
		if node.State == STATE_ALIVE {
			res = append(res, nodes[i])
		}
	}
	return res
}

// The first R nodes with the lowest ID are selected as metadata replicas.
func GetMetadataReplicaNodes(count int) []*NodeInfo {
	aliveNodes := GetAliveNodes(nodes)
	return aliveNodes[:min(count, len(aliveNodes))]
}

// The hash of the filename is selected as primary replica. The next R-1 successors are selected as backup replicas.
func GetReplicaNodes(filename string, count int) []*NodeInfo {
	aliveNodes := GetAliveNodes(nodes)
	if len(aliveNodes) < count {
		return aliveNodes
	}

	hash := GetHash(filename, len(aliveNodes))
	replicas := []*NodeInfo{}

	for i := 0; i < count; i++ {
		j := (hash + i) % len(aliveNodes)
		replicas = append(replicas, aliveNodes[j])
	}

	return replicas
}

// To handle tcp connection
func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Create a buffer to hold incoming data
	buffer := make([]byte, 1024)

	for {
		// Read data from the client
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Client %s disconnected\n", conn.RemoteAddr())
			return
		}

		request := string(buffer[:n])
		fmt.Printf("Received request from %s: %s\n", conn.RemoteAddr(), request)

		lines := strings.Split(request, "\n")
		tokens := strings.Split(lines[0], " ")
		verb := strings.ToUpper(tokens[0])

		if strings.Index(verb, "EXIT") == 0 {
			return
		}

		response := []byte("OK\n")

		// Echo the data back to the client
		_, err = conn.Write(response)
		if err != nil {
			fmt.Printf("Error writing to client: %s\n", err)
			return
		}
	}
}

// Start a TCP server on given port
func startTCPServer(port string) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("Error starting server: %s\n", err)
		return
	}
	defer listener.Close()

	fmt.Printf("Server is listening on port %s...\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %s\n", err)
			continue
		}

		fmt.Printf("Accepted connection from %s\n", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func main() {
	// fmt.Println("Hello world")
	//
	// for i := 0; i < 5; i++ {
	// 	fmt.Printf("Hash for 'file%d.txt': %d\n", i, GetHash(fmt.Sprintf("file%d.txt", i), len(nodes)))
	// }

	// Check for command-line arguments
	if len(os.Args) != 2 {
		fmt.Println("Usage: ./main <port>")
		return
	}

	// Get the port number from the command-line arguments
	port := os.Args[1]

	startTCPServer(port)
}
