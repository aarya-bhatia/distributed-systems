package filesystem

import (
	"container/heap"
	"cs425/common"
	"cs425/priqueue"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	ACTION_DOWNLOAD_FILE = 0x10
	ACTION_UPLOAD_FILE   = 0x20

	ACTION_DOWNLOAD_BLOCK = 0x11
	ACTION_UPLOAD_BLOCK   = 0x21

	ACTION_ADD_BLOCK    = 0x30
	ACTION_REMOVE_BLOCK = 0x40

	STATE_ALIVE  = 0
	STATE_FAILED = 1
)

type File struct {
	Filename  string
	Version   int
	FileSize  int
	NumBlocks int
}

// A block of file
type Block struct {
	Size int
	Data []byte
}

type Request struct {
	Action int
	Client net.Conn
	Name   string
	Size   int
}

type Server struct {
	Info          common.Node
	Files         map[string]*File    // Files stored by system
	Storage       map[string]*Block   // In memory data storage
	NodesToBlocks map[int][]string    // Maps node to the blocks they are storing
	BlockToNodes  map[string][]int    // Maps block to list of nodes that store the block
	Nodes         map[int]common.Node // Set of alive nodes
	InputChannel  chan string
	Requests      chan Request
	Mutex         sync.Mutex

	// ackChannel chan string
}

var Log *common.Logger = common.Log

func (s *Server) HandleNodeJoin(node *common.Node) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	Log.Debug("node joined: ", *node)
	s.Nodes[node.ID] = *node
	s.NodesToBlocks[node.ID] = []string{}
	s.Rebalance()
}

func (s *Server) HandleNodeLeave(node *common.Node) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	Log.Debug("node left: ", *node)
	delete(s.Nodes, node.ID)
	for _, block := range s.NodesToBlocks[node.ID] {
		for i := 0; i < len(s.BlockToNodes[block]); i++ {
			nodes := s.BlockToNodes[block]
			if nodes[i] == node.ID {
				s.BlockToNodes[block] = common.RemoveIndex(nodes, i)
				break
			}
		}
	}
	delete(s.NodesToBlocks, node.ID)
	s.Rebalance()
}

func NewServer(info common.Node) *Server {
	server := new(Server)
	server.Info = info
	server.Files = make(map[string]*File)
	server.Storage = make(map[string]*Block)
	server.NodesToBlocks = make(map[int][]string)
	server.BlockToNodes = make(map[string][]int)
	server.InputChannel = make(chan string)
	server.Nodes = make(map[int]common.Node)
	server.Nodes[info.ID] = info
	server.Requests = make(chan Request)

	return server
}

// Node with smallest ID
func (s *Server) GetLeaderNode(count int) int {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	pq := make(priqueue.PriorityQueue, 0)
	heap.Init(&pq)

	for ID := range s.Nodes {
		heap.Push(&pq, &priqueue.Item{Key: ID})
	}

	item := heap.Pop(&pq).(*priqueue.Item)
	return item.Key
}

// The first R nodes with the lowest ID are selected as metadata replicas.
func (s *Server) GetMetadataReplicaNodes(count int) []int {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	pq := make(priqueue.PriorityQueue, 0)
	heap.Init(&pq)

	for ID := range s.Nodes {
		heap.Push(&pq, &priqueue.Item{Key: ID})
	}

	res := []int{}
	for r := 0; r < count && pq.Len() > 0; r++ {
		item := heap.Pop(&pq).(*priqueue.Item)
		res = append(res, item.Key)
	}

	return res
}

// Get the `count` nearest nodes to the file hash
func (s *Server) GetReplicaNodes(filename string, count int) []int {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	fileHash := common.GetHash(filename, len(common.Cluster))
	pq := make(priqueue.PriorityQueue, 0)
	heap.Init(&pq)

	for ID := range s.Nodes {
		distance := ID - fileHash
		if distance < 0 {
			distance = -distance
		}

		heap.Push(&pq, &priqueue.Item{Key: distance, Value: ID})
	}

	res := []int{}
	for r := 0; r < count && pq.Len() > 0; r++ {
		item := heap.Pop(&pq).(*priqueue.Item)
		res = append(res, item.Value)
	}

	return res
}

// Receive requests from clients and send them to the Requests channel
func (server *Server) listenerRoutine(listener net.Listener) {
	buffer := make([]byte, common.MIN_BUFFER_SIZE)

	for {
		conn, err := listener.Accept()
		if err != nil {
			Log.Warnf("Error accepting connection: %s\n", err)
			continue
		}

		Log.Debugf("Accepted connection from %s\n", conn.RemoteAddr())

		n, err := conn.Read(buffer)

		if err != nil {
			Log.Warnf("Client %s disconnected\n", conn.RemoteAddr())
			return
		}

		request := string(buffer[:n])

		Log.Debugf("Received request from %s: %s\n", conn.RemoteAddr(), request)

		lines := strings.Split(request, "\n")
		tokens := strings.Split(lines[0], " ")
		verb := tokens[0]

		if verb == "UPLOAD_FILE" {
			filename := tokens[1]
			filesize, err := strconv.Atoi(tokens[2])
			if err != nil {
				Log.Warn(err)
				return
			}

			server.Requests <- Request{
				Action: ACTION_UPLOAD_FILE,
				Client: conn,
				Name:   filename,
				Size:   filesize,
			}

		} else if verb == "DOWNLOAD_FILE" {
			filename := tokens[1]
			server.Requests <- Request{
				Action: ACTION_DOWNLOAD_FILE,
				Client: conn,
				Name:   filename,
			}

		} else if verb == "UPLOAD" { // To upload block at replica
			blockName := tokens[1]
			blockSize, err := strconv.Atoi(tokens[2])
			if err != nil {
				Log.Warn(err)
				return
			}
			server.Requests <- Request{
				Action: ACTION_UPLOAD_BLOCK,
				Client: conn,
				Name:   blockName,
				Size:   blockSize,
			}

		} else if verb == "DOWNLOAD" { // To download block at replica
			blockName := tokens[1]

			server.Requests <- Request{
				Action: ACTION_DOWNLOAD_BLOCK,
				Client: conn,
				Name:   blockName,
			}
		} else {
			Log.Warn("Unknown verb: ", verb)
			conn.Write([]byte("ERROR\nUnknown verb"))
			conn.Close()
		}
	}
}

func (server *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Info.TCPPort))
	if err != nil {
		Log.Fatal("Error starting server", err)
	}
	defer listener.Close()

	go server.listenerRoutine(listener)

	Log.Infof("TCP Server is listening on port %d...\n", server.Info.TCPPort)

	for {
		task := <-server.Requests

		switch task.Action {

		case ACTION_UPLOAD_FILE:
			if !server.UploadFile(task.Client, task.Name, task.Size, 1) {
				task.Client.Write([]byte("ERROR\n"))
			}

		case ACTION_DOWNLOAD_FILE:
			server.DownloadFile(task.Client, task.Name)

		case ACTION_UPLOAD_BLOCK:
			tokens := strings.Split(task.Name, ":")
			filename := tokens[0]
			version, err := strconv.Atoi(tokens[1])
			if err != nil {
				Log.Warn(err)
				break
			}
			blockNum, err := strconv.Atoi(tokens[2])
			if err != nil {
				Log.Warn(err)
				break
			}
			if !server.UploadBlock(task.Client, filename, version, blockNum, task.Size) {
				task.Client.Write([]byte("ERROR\n"))
				return
			}

		case ACTION_DOWNLOAD_BLOCK:
			server.DownloadBlockResponse(task.Client, task.Name)
		}

		task.Client.Close()
	}
}

// Print file system metadata information to stdout
func (server *Server) PrintFileMetadata() {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Filename", "Version", "Block", "Nodes"})

	rows := []table.Row{}

	for blockName, nodeIds := range server.BlockToNodes {
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

// To handle replicas after a node fails or rejoins
func (s *Server) Rebalance() {
	// m := map[int]bool{}
	//
	//	for _, node := range nodes {
	//		m[node.ID] = node.State == STATE_ALIVE
	//	}
	//
	// // Get affected replicas
	// // Add tasks to replicate the affected blocks to queue
	//
	//	for _, file := range files {
	//		expectedReplicas := GetReplicaNodes(file.Filename, REPLICA_FACTOR)
	//		for i := 0; i < file.NumBlocks; i++ {
	//			blockInfo := BlockInfo{Filename: file.Filename, Version: file.Version, ID: i}
	//			_, ok := blockToNodes[blockInfo]
	//			if !ok {
	//				for _, replica := range expectedReplicas {
	//					AddTask(UPDATE_BLOCK, replica, blockInfo)
	//				}
	//			} else {
	//				// TODO: AddTask for Intersection nodes
	//			}
	//		}
	//	}
}

func (server *Server) HandleCommand(command string) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	if command == "help" {
		fmt.Println("ls: Display metadata table")
		fmt.Println("info: Display server info")
		fmt.Println("files: Display list of files")
		fmt.Println("blocks: Display list of blocks")
	} else if command == "ls" {
		server.PrintFileMetadata()
	} else if command == "files" {
		for name, block := range server.Storage {
			tokens := strings.Split(name, ":")
			fmt.Printf("File %s, version %s, block %s, size %d\n", tokens[0], tokens[1], tokens[2], block.Size)
		}
	} else if command == "blocks" {
		for name, arr := range server.BlockToNodes {
			fmt.Printf("Block %s: ", name)
			for _, node := range arr {
				fmt.Printf("%d ", node)
			}
			fmt.Print("\n")
		}
	} else if command == "info" {
		fmt.Printf("ID: %d, Hostname: %s, Port: %d\n", server.Info.ID, server.Info.Hostname, server.Info.TCPPort)
		fmt.Printf("Num files: %d, Num blocks: %d, Num nodes: %d\n", len(server.Files), len(server.BlockToNodes), len(server.NodesToBlocks))
	}
}
