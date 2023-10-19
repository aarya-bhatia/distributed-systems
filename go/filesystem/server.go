package filesystem

import (
	"cs425/common"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
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
	Nodes     map[string]bool // Set of alive nodes
	Mutex     sync.Mutex

	// A block is represented as "filename:version:blocknum"
	NodesToBlocks map[string][]string // Maps node ID to the blocks they are storing
	BlockToNodes  map[string][]string // Maps block to list of node IDs that store the block

	FileQueues map[string]*Queue // Handle read/write operations for each file
}

const (
	DOWNLOAD_FILE = 1
	UPLOAD_FILE   = 2
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
	server.FileQueues = make(map[string]*Queue)
	server.Nodes = make(map[string]bool)
	server.Nodes[server.ID] = true

	return server
}

func (server *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Port))
	if err != nil {
		Log.Fatal("Error starting server: ", err)
	}

	Log.Infof("TCP Server is listening on port %d...\n", server.Port)

	if GetLeaderNode(server.GetAliveNodes()) == server.ID {
		Log.Info("Starting master routine...")
		go server.pollTasks()
	}

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

// Get or create a queue to handle requests for given file
func (server *Server) getQueue(filename string) *Queue {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
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
			// conn.Write([]byte("ERROR\nFilename not specified\n"))
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

		uploadBlock(server.Directory, conn, blockName, blockSize)

	} else if verb == "DOWNLOAD" { // To download block at replica
		if len(tokens) != 2 {
			conn.Write([]byte("ERROR\nUsage: DOWNLOAD <blockname>\n"))
			conn.Close()
			return
		}

		downloadBlock(server.Directory, conn, tokens[1])

	} else if verb == "ADD_BLOCK" {
		if len(tokens) != 4 {
			conn.Close()
			return
		}
		blockName := tokens[1]
		blockSize, err := strconv.Atoi(tokens[2])
		if err != nil {
			conn.Close()
			return
		}
		source := tokens[3]
		server.replicateBlock(conn, blockName, blockSize, source)

	} else {
		Log.Warn("Unknown verb: ", verb)
		conn.Write([]byte("ERROR\nUnknown verb\n"))
		conn.Close()
	}
}

// To handle replicas after a node fails or rejoins
func (s *Server) rebalance() {
	// TODO
}

// Download a block from source node to replicate it at current node
func (server *Server) replicateBlock(client net.Conn, blockName string, blockSize int, source string) {
	defer client.Close()
	Log.Debugf("To replicate block %s from host %s\n", blockName, source)
	request := fmt.Sprintf("DOWNLOAD %s\n", blockName)
	conn, err := net.Dial("tcp", source)
	if err != nil {
		client.Write([]byte("ERROR\n"))
		return
	}

	defer conn.Close()

	_, err = conn.Write([]byte(request))
	if err != nil {
		client.Write([]byte("ERROR\n"))
		return
	}

	buffer := make([]byte, common.BLOCK_SIZE)
	size := 0
	for size < blockSize {
		n, err := conn.Read(buffer[size:])
		if err != nil {
			client.Write([]byte("ERROR\n"))
			return
		}
		if n == 0 {
			break
		}
		size += n
	}

	if !common.WriteFile(server.Directory, blockName, buffer, size) {
		client.Write([]byte("ERROR\n"))
		return
	}

	client.Write([]byte("OK\n"))
}

func (s *Server) HandleNodeJoin(info *common.Node) {
	s.Mutex.Lock()
	node := fmt.Sprintf("%s:%d", info.Hostname, info.TCPPort)
	Log.Debug("node left: ", node)
	s.Nodes[node] = true
	s.NodesToBlocks[node] = []string{}
	s.Mutex.Unlock()
	s.rebalance()
}

func (s *Server) HandleNodeLeave(info *common.Node) {
	s.Mutex.Lock()
	node := fmt.Sprintf("%s:%d", info.Hostname, info.TCPPort)
	Log.Debug("node left: ", node)
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
	s.Mutex.Unlock()
	s.rebalance()
}
