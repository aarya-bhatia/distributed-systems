package filesystem

import (
	"cs425/common"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
)

type File struct {
	Filename  string
	Version   int
	FileSize  int
	NumBlocks int
}

type Request struct {
	Action int
	Client net.Conn
	Name   string
	Size   int
}

type Server struct {
	Hostname  string
	Port      int
	ID        string          // hostname:port
	Directory string          // Path to save blocks on disk
	Files     map[string]File // Files stored by system

	// A block is represented as "filename:version:blocknum"

	NodesToBlocks map[string][]string // Maps node ID to the blocks they are storing
	BlockToNodes  map[string][]string // Maps block to list of node IDs that store the block

	FileQueues map[string]*Queue // Handle read/write operations for each file

	Nodes        map[string]bool // Set of alive nodes
	InputChannel chan string     // Receive commands from stdin
	Requests     chan *Request   // Receive requests from listener thread
	NodeJoins    chan string
	NodeLeaves   chan string
}

const (
	DOWNLOAD_FILE = 1
	UPLOAD_FILE   = 2

	UPLOAD_BLOCK   = 3
	DOWNLOAD_BLOCK = 4

	ADD_BLOCK    = 5
	REMOVE_BLOCK = 6
)

var Log *common.Logger = common.Log

func NewServer(info common.Node, dbDirectory string) *Server {
	server := new(Server)
	server.Hostname = info.Hostname
	server.Port = info.TCPPort
	server.ID = fmt.Sprintf("%s:%d", info.Hostname, info.TCPPort)
	server.Directory = dbDirectory
	server.Files = make(map[string]File)
	server.NodesToBlocks = make(map[string][]string)
	server.BlockToNodes = make(map[string][]string)
	server.InputChannel = make(chan string)
	server.FileQueues = make(map[string]*Queue)
	server.Nodes = make(map[string]bool)
	server.Requests = make(chan *Request)
	server.NodeJoins = make(chan string)
	server.NodeLeaves = make(chan string)
	server.Nodes[server.ID] = true

	return server
}

func (s *Server) HandleNodeJoin(node *common.Node) {
	Log.Debug("node joined: ", *node)
	s.NodeJoins <- fmt.Sprintf("%s:%d", node.Hostname, node.TCPPort)
}

func (s *Server) HandleNodeLeave(node *common.Node) {
	Log.Debug("node left: ", *node)
	Log.Debug("node joined: ", *node)
	s.NodeLeaves <- fmt.Sprintf("%s:%d", node.Hostname, node.TCPPort)
}

// Get or create a queue to handle requests for given file
func (server *Server) getQueue(filename string) *Queue {
	q, ok := server.FileQueues[filename]
	if !ok {
		q = new(Queue)
		server.FileQueues[filename] = q
	}
	return q
}

func (server *Server) handleConnection(conn net.Conn) {
	buffer := make([]byte, common.MIN_BUFFER_SIZE)
	n, err := conn.Read(buffer)
	if err != nil {
		Log.Warnf("Client %s disconnected\n", conn.RemoteAddr())
		return
	}

	Log.Debugf("Received request from %s: %s\n", conn.RemoteAddr(), string(buffer[:n]))
	lines := strings.Split(string(buffer[:n]), "\n")
	tokens := strings.Split(lines[0], " ")
	verb := tokens[0]

	if verb == "UPLOAD_FILE" { // To read a file from client
		if len(tokens) != 3 {
			conn.Write([]byte("ERROR\nUsage: UPLOAD_FILE <filename> <filesize>\n"))
			conn.Close()
			return
		}

		filename := tokens[1]

		filesize, err := strconv.Atoi(tokens[2])
		if err != nil || filesize <= 0 {
			conn.Write([]byte("ERROR\nFile size must be a positive number\n"))
			conn.Close()
			return
		}

		server.getQueue(filename).PushWrite(&Request{
			Action: UPLOAD_FILE,
			Client: conn,
			Name:   filename,
			Size:   filesize,
		})

	} else if verb == "DOWNLOAD_FILE" { // To write a file to a client
		if len(tokens) != 2 {
			conn.Write([]byte("ERROR\nFilename not specified\n"))
			conn.Close()
			return
		}

		filename := tokens[1]

		Log.Debugf("Received download request for file %s\n", filename)
		server.getQueue(filename).PushRead(&Request{
			Action: DOWNLOAD_FILE,
			Client: conn,
			Name:   filename,
		})

	} else if verb == "UPLOAD" { // To upload block at replica
		if len(tokens) != 3 {
			conn.Write([]byte("ERROR\nUsage: UPLOAD_BLOCK <blockname> <blocksize>\n"))
			conn.Close()
			return
		}

		blockName := tokens[1]
		blockSize, err := strconv.Atoi(tokens[2])
		if err != nil || blockSize <= 0 {
			conn.Write([]byte("ERROR\nBlock size must be a positive number\n"))
			conn.Close()
			return
		}

		if blockSize > common.BLOCK_SIZE {
			conn.Write([]byte(fmt.Sprintf("ERROR\nMaximum block size is %d\n", common.BLOCK_SIZE)))
			conn.Close()
			return
		}

		server.Requests <- &Request{
			Action: UPLOAD_BLOCK,
			Client: conn,
			Name:   blockName,
			Size:   blockSize,
		}

	} else if verb == "DOWNLOAD" { // To download block at replica
		if len(tokens) != 2 {
			conn.Write([]byte("ERROR\nUsage: DOWNLOAD_BLOCK <blockname>\n"))
			conn.Close()
			return
		}

		blockName := tokens[1]

		server.Requests <- &Request{
			Action: DOWNLOAD_BLOCK,
			Client: conn,
			Name:   blockName,
		}

	} else {
		Log.Warn("Unknown verb: ", verb)
		conn.Write([]byte("ERROR\nUnknown verb\n"))
		conn.Close()
	}
}

// Receive requests from clients and send them to the Requests channel
func (server *Server) listenerRoutine(listener net.Listener) {

	for {
		conn, err := listener.Accept()
		if err != nil {
			Log.Warnf("Error accepting connection: %s\n", err)
			continue
		}

		Log.Debugf("Accepted connection from %s\n", conn.RemoteAddr())

		go server.handleConnection(conn)
	}
}

func (server *Server) getTasks() {
	for _, queue := range server.FileQueues {
		task := queue.TryPop()
		if task != nil {
			server.Requests <- task
		}
	}
}

func (server *Server) getFileVersion(filename string) int {
	if file, ok := server.Files[filename]; ok {
		return file.Version
	}

	return 0
}

func (server *Server) handleRequest(task *Request) {
	switch task.Action {

	case UPLOAD_FILE:
		if server.sendUploadFileMetadata(task.Client, task.Name, int64(task.Size)) {

			newFile := File{
				Filename:  task.Name,
				FileSize:  task.Size,
				Version:   server.getFileVersion(task.Name) + 1,
				NumBlocks: common.GetNumFileBlocks(int64(task.Size)),
			}

			go server.finishUpload(task.Client, newFile)

		} else {
			server.getQueue(task.Name).Done()
			task.Client.Close()
		}

	case DOWNLOAD_FILE:
		if server.sendDownloadFileMetadata(task.Client, task.Name) {
			go server.finishDownload(task.Client, task.Name)
		} else {
			server.getQueue(task.Name).Done()
			task.Client.Close()
		}

	case UPLOAD_BLOCK:
		go server.handleUploadBlockRequest(task)

	case DOWNLOAD_BLOCK:
		go server.handleDownloadBlockRequest(task)
	}
}

func (server *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Port))
	if err != nil {
		Log.Fatal("Error starting server: ", err)
	}

	Log.Infof("TCP Server is listening on port %d...\n", server.Port)
	go server.listenerRoutine(listener)
	defer listener.Close()

	for {
		select {
		case task := <-server.Requests:
			server.handleRequest(task)

		case task := <-server.InputChannel:
			server.handleCommand(task)

		case node := <-server.NodeJoins:
			server.addNode(node)

		case node := <-server.NodeLeaves:
			server.removeNode(node)

		case <-time.After(time.Second):
			go server.getTasks()
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

func (server *Server) handleCommand(command string) {
	if command == "help" {
		fmt.Println("ls: Display metadata table")
		fmt.Println("info: Display server info")
		fmt.Println("files: Display list of files")
	} else if command == "ls" {
		server.printFileMetadata()
	} else if command == "files" {
		if server.GetLeaderNode() == server.ID {
			fmt.Println("== LEADER NODE ==")
			for _, f := range server.Files {
				fmt.Printf("File name:%s, version:%d, size:%d, numBlocks:%d\n", f.Filename, f.Version, f.FileSize, f.NumBlocks)
			}
		} else {
			files, err := getFilesInDirectory(server.Directory)
			if err != nil {
				Log.Warn(err)
				return
			}
			for _, f := range files {
				tokens := strings.Split(f.Name, ":")
				fmt.Printf("File %s, version %s, block %s, size %d\n", tokens[0], tokens[1], tokens[2], f.Size)
			}
		}
	} else if command == "info" {
		fmt.Printf("Hostname: %s, Port: %d\n", server.Hostname, server.Port)
		fmt.Printf("Num files: %d, Num blocks: %d, Num nodes: %d\n", len(server.Files), len(server.BlockToNodes), len(server.NodesToBlocks))
	}
}

func (s *Server) addNode(node string) {
	s.Nodes[node] = true
	s.NodesToBlocks[node] = []string{}
	s.rebalance()
}

func (s *Server) removeNode(node string) {
	delete(s.Nodes, node)
	for _, block := range s.NodesToBlocks[node] {
		arr := s.BlockToNodes[block]
		for i := 0; i < len(arr); i++ {
			if arr[i] == node {
				// Remove element at index i
				arr[i], arr[len(arr)-1] = arr[len(arr)-1], arr[i]
				s.BlockToNodes[block] = arr[:len(arr)-1]
				break
			}
		}
	}
	delete(s.NodesToBlocks, node)
	s.rebalance()
}
