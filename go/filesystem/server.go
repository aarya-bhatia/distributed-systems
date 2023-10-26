package filesystem

import (
	"cs425/common"
	"fmt"
	"net"
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
	DELETE_FILE   = 3
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
	server.NodesToBlocks[server.ID] = []string{}

	return server
}

// Start tcp server on configured port and listen for connections
// Launches a routine for each connection
// Starts routine to poll read/write tasks from queue
func (server *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Port))
	if err != nil {
		Log.Fatal("Error starting server: ", err)
	}

	Log.Infof("TCP Server is listening on port %d...\n", server.Port)

	go server.pollTasks()
	go server.startRebalanceRoutine()

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

func (s *Server) HandleNodeJoin(info *common.Node) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	node := fmt.Sprintf("%s:%d", info.Hostname, info.TCPPort)
	Log.Debug("node join: ", node)

	s.Nodes[node] = true
	s.NodesToBlocks[node] = []string{}
}

func (s *Server) HandleNodeLeave(info *common.Node) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

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
				Log.Debugf("block %s has %d replicas", block, len(s.BlockToNodes[block]))
				break
			}
		}
	}

	delete(s.NodesToBlocks, node)
}

/* func (s *Server) GetBlockSize(blockName string) (int, bool) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	tokens := strings.Split(blockName, ":")
	filename := tokens[0]
	version, err := strconv.Atoi(tokens[1])
	if err != nil {
		return 0, false
	}

	blockNum, err := strconv.Atoi(tokens[2])
	if err != nil {
		return 0, false
	}

	file, ok := s.Files[filename]
	if !ok {
		return 0, false
	}

	if file.Version != version {
		Log.Warn("Invalid block version", blockName)
		return 0, false
	}

	if blockNum >= file.NumBlocks {
		Log.Warn("Invalid block number", blockName)
		return 0, false
	}

	if blockNum == file.NumBlocks-1 {
		r := file.FileSize % common.BLOCK_SIZE
		if r == 0 {
			return common.BLOCK_SIZE, true
		}

		return r, true
	}

	return common.BLOCK_SIZE, true
} */
