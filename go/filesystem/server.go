package filesystem

import (
	"cs425/common"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
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
	Action    int
	Client    net.Conn
	Name      string
	Size      int
	Timestamp int64
}

type Server struct {
	Info          common.Node
	Files         map[string]*File    // Files stored by system
	Storage       map[string]*Block   // In memory data storage
	NodesToBlocks map[int][]string    // Maps node to the blocks they are storing
	BlockToNodes  map[string][]int    // Maps block to list of nodes that store the block
	Nodes         map[int]common.Node // Set of alive nodes
	InputChannel  chan string         // Receive commands from stdin
	FileQueues    map[string]*RWQueue // Handle read/write operations for each file
	// Queue         *RWQueue
	// ackChannel chan string
}

const (
	DOWNLOAD_FILE  = 1
	UPLOAD_FILE    = 2
	UPLOAD_BLOCK   = 3
	DOWNLOAD_BLOCK = 4
	ADD_BLOCK      = 5
	REMOVE_BLOCK   = 6
)

var Log *common.Logger = common.Log

func (s *Server) HandleNodeJoin(node *common.Node) {
	Log.Debug("node joined: ", *node)
	s.Nodes[node.ID] = *node
	s.NodesToBlocks[node.ID] = []string{}
	s.rebalance()
}

func (s *Server) HandleNodeLeave(node *common.Node) {
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
	s.rebalance()
}

func NewServer(info common.Node) *Server {
	server := new(Server)
	server.Info = info
	server.Files = make(map[string]*File)
	server.Storage = make(map[string]*Block)
	server.NodesToBlocks = make(map[int][]string)
	server.BlockToNodes = make(map[string][]int)
	server.InputChannel = make(chan string)
	server.FileQueues = make(map[string]*RWQueue)
	server.Nodes = make(map[int]common.Node)

	server.Nodes[info.ID] = info

	return server
}

// Receive requests from clients and send them to the Requests channel
func (server *Server) listenerRoutine(listener net.Listener, requests chan *Request) {
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

		timestamp := time.Now().UnixNano()

		lines := strings.Split(request, "\n")
		tokens := strings.Split(lines[0], " ")
		verb := tokens[0]

		if verb == "UPLOAD_FILE" { // To read a file from client
			filename := tokens[1]
			filesize, err := strconv.Atoi(tokens[2])
			if err != nil {
				Log.Warn(err)
				return
			}

			requests <- (&Request{
				Action:    UPLOAD_FILE,
				Client:    conn,
				Name:      filename,
				Size:      filesize,
				Timestamp: timestamp,
			})

		} else if verb == "DOWNLOAD_FILE" { // To write a file to a client
			filename := tokens[1]

			requests <- (&Request{
				Action:    DOWNLOAD_FILE,
				Client:    conn,
				Name:      filename,
				Timestamp: timestamp,
			})

		} else if verb == "UPLOAD" { // To upload block at replica
			blockName := tokens[1]
			blockSize, err := strconv.Atoi(tokens[2])
			if err != nil {
				Log.Warn(err)
				return
			}

			requests <- (&Request{
				Action:    UPLOAD_BLOCK,
				Client:    conn,
				Name:      blockName,
				Size:      blockSize,
				Timestamp: timestamp,
			})

		} else if verb == "DOWNLOAD" { // To download block at replica
			blockName := tokens[1]

			requests <- (&Request{
				Action:    DOWNLOAD_BLOCK,
				Client:    conn,
				Name:      blockName,
				Timestamp: timestamp,
			})

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

	Log.Infof("TCP Server is listening on port %d...\n", server.Info.TCPPort)

	requests := make(chan *Request)

	go server.listenerRoutine(listener, requests)

	// go func() {
	// 	requests <- server.Queue.Pop()
	// }()

	for {
		select {
		case task := <-requests:
			server.handleRequest(task)
			// go func() {
			// 	if task.Action == DOWNLOAD_BLOCK || task.Action == DOWNLOAD_FILE {
			// 		server.Queue.ReadDone()
			// 	} else if task.Action == UPLOAD_BLOCK || task.Action == UPLOAD_FILE {
			// 		server.Queue.WriteDone()
			// 	}
			// }()

		case task := <-server.InputChannel:
			server.handleCommand(task)
		}
	}
}

// Print file system metadata information to stdout
func (server *Server) printFileMetadata() {
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
func (s *Server) rebalance() {
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

func (server *Server) handleRequest(task *Request) {
	defer task.Client.Close()

	switch task.Action {

	case UPLOAD_FILE:
		if !server.UploadFile(task.Client, task.Name, task.Size, 1) {
			task.Client.Write([]byte("ERROR\n"))
		}

	case DOWNLOAD_FILE:
		server.DownloadFile(task.Client, task.Name)

	case UPLOAD_BLOCK:
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

	case DOWNLOAD_BLOCK:
		server.DownloadBlockResponse(task.Client, task.Name)

	default:
		Log.Warn("Unknown action")
	}
}

func (server *Server) handleCommand(command string) {
	if command == "help" {
		fmt.Println("ls: Display metadata table")
		fmt.Println("info: Display server info")
		fmt.Println("files: Display list of files")
		fmt.Println("blocks: Display list of blocks")
	} else if command == "ls" {
		server.printFileMetadata()
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
