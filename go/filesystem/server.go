package filesystem

import (
	"cs425/common"
	"fmt"
	log "github.com/sirupsen/logrus"
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
	Hostname   string
	Port       int
	ID         string          // hostname:port
	Directory  string          // Path to save blocks on disk
	Files      map[string]File // Files stored by system
	Nodes      map[string]bool // Set of alive nodes
	Mutex      sync.Mutex
	FileQueues map[string]*Queue // Handle read/write operations for each file

	// A block is represented as "filename:version:blocknum"
	NodesToBlocks map[string][]string // Maps node ID to the blocks they are storing
	BlockToNodes  map[string][]string // Maps block to list of node IDs that store the block
}

const (
	DOWNLOAD_FILE = 1
	UPLOAD_FILE   = 2
	DELETE_FILE   = 3
)

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
		log.Fatal("Error starting server: ", err)
	}

	log.Infof("TCP File Server is listening on port %d...\n", server.Port)

	go server.pollTasks()
	go server.startRebalanceRoutine()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

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
	node := fmt.Sprintf("%s:%d", info.Hostname, info.TCPPort)
	log.Debug("node join: ", node)
	s.Nodes[node] = true
	s.NodesToBlocks[node] = []string{}
	s.Mutex.Unlock()

	s.replicateMetadata()
}

func (s *Server) HandleNodeLeave(info *common.Node) {
	s.Mutex.Lock()
	node := fmt.Sprintf("%s:%d", info.Hostname, info.TCPPort)
	log.Debug("node left: ", node)
	for _, block := range s.NodesToBlocks[node] {
		s.BlockToNodes[block] = common.RemoveElement(s.BlockToNodes[block], node)
	}
	delete(s.NodesToBlocks, node)
	delete(s.Nodes, node)
	s.Mutex.Unlock()

	s.replicateMetadata()
}

