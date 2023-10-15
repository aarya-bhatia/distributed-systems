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

const (
	REQUEST_READ  = 0
	REQUEST_WRITE = 1
	STATE_ALIVE  = 0
	STATE_FAILED = 1
)

var nodes []*NodeInfo = []*NodeInfo{
	{ID: 0, Hostname: "localhost", Port: 5000, State: STATE_ALIVE},
	{ID: 1, Hostname: "localhost", Port: 5001, State: STATE_ALIVE},
	{ID: 2, Hostname: "localhost", Port: 5002, State: STATE_ALIVE},
	{ID: 3, Hostname: "localhost", Port: 5003, State: STATE_ALIVE},
	{ID: 4, Hostname: "localhost", Port: 5004, State: STATE_ALIVE},
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: ./main <ID>")
		return
	}

	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})

	log.SetReportCaller(false)
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)

	ID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	server := NewServer(ID)

	go StdinListener(server)

	StartServer(server)
}

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
