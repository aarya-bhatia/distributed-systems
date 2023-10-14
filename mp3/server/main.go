package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	table "github.com/jedib0t/go-pretty/v6/table"
	log "github.com/sirupsen/logrus"
)

type Block struct {
	Size int
	Data []byte
}

type File struct {
	Filename  string
	Version   int
	FileSize  int
	NumBlocks int
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

type Server struct {
	files         map[string]*File  // Files stored by system
	storage       map[string]*Block // In memory data storage
	nodesToBlocks map[int][]string  // Maps node to the blocks they are storing
	blockToNodes  map[string][]int  // Maps block to list of nodes that store the block
	info          *NodeInfo         // Info about current server
	// var ackChannel chan string
	// var failureDetectorChannel chan string
	// var queue []*Request                 // A queue of requests
}

const (
	ENV            = "DEV"
	REQUEST_READ   = 0
	REQUEST_WRITE  = 1
	STATE_ALIVE    = 0
	STATE_FAILED   = 1
	DEFAULT_PORT   = 5000
	REPLICA_FACTOR = 1
	// BLOCK_SIZE = 4096 * 1024 // 4 MB
	BLOCK_SIZE      = 16
	MIN_BUFFER_SIZE = 1024
)

var nodes []*NodeInfo = []*NodeInfo{
	{ID: 1, Hostname: "localhost", Port: 5000, State: STATE_ALIVE},
	{ID: 2, Hostname: "localhost", Port: 5001, State: STATE_ALIVE},
	{ID: 3, Hostname: "localhost", Port: 5002, State: STATE_ALIVE},
	{ID: 4, Hostname: "localhost", Port: 5003, State: STATE_ALIVE},
}

func GetBlockName(filename string, version int, blockNum int) string {
	return fmt.Sprintf("%s:%d:%d", filename, version, blockNum)
}

// const UPDATE_BLOCK = 0
// const REMOVE_BLOCK = 1
// func AddTask(taskType int, node *NodeInfo, args interface{}) {
// }
// // To handle replicas after a node fails or rejoins
// func rebalance() {
// 	m := map[int]bool{}
// 	for _, node := range nodes {
// 		m[node.ID] = node.State == STATE_ALIVE
// 	}
// 	// Get affected replicas
// 	// Add tasks to replicate the affected blocks to queue
//
// 	for _, file := range files {
// 		expectedReplicas := GetReplicaNodes(file.Filename, REPLICA_FACTOR)
// 		for i := 0; i < file.NumBlocks; i++ {
// 			blockInfo := BlockInfo{Filename: file.Filename, Version: file.Version, ID: i}
// 			_, ok := blockToNodes[blockInfo]
// 			if !ok {
// 				for _, replica := range expectedReplicas {
// 					AddTask(UPDATE_BLOCK, replica, blockInfo)
// 				}
// 			} else {
// 				// TODO: AddTask for Intersection nodes
// 			}
// 		}
// 	}
// }

// Print file system metadata information to stdout
func PrintFileMetadata(server *Server) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Filename", "Version", "Block", "Nodes"})

	rows := []table.Row{}

	for blockName, nodeIds := range server.blockToNodes {
		tokens := strings.Split(blockName, ":")
		filename, version, block := tokens[0], tokens[1], tokens[2]

		for _, id := range nodeIds {
			rows = append(rows, table.Row{
				filename,
				version,
				block,
				id,
			})
		}
	}

	t.AppendRows(rows)
	t.AppendSeparator()
	t.SetStyle(table.StyleLight)
	t.Render()
}

// Read requests from stdin and send them to request channel
func StdinListener(server *Server) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		if command == "ls" {
			PrintFileMetadata(server)
		}
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: ./main <ID>")
		return
	}

	ID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	server := new(Server)

	for _, node := range nodes {
		if node.ID == ID {
			server.info = node
			break
		}
	}

	if server.info == nil {
		log.Fatal("Unknown Server ID")
	}

	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})

	log.SetReportCaller(false)
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)

	server.files = make(map[string]*File)
	server.storage = make(map[string]*Block)
	server.nodesToBlocks = make(map[int][]string)
	server.blockToNodes = make(map[string][]int)

	go StdinListener(server)

	StartServer(server)
}
