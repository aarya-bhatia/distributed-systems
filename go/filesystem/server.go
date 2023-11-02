package filesystem

import (
	"cs425/common"
	"fmt"
	"net"
	"os"
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
	Hostname            string
	Port                int
	ID                  string          // hostname:port
	Directory           string          // Path to save blocks on disk
	Files               map[string]File // Files stored by system
	Nodes               map[string]bool // Set of alive nodes
	Mutex               sync.Mutex
	FileQueues          map[string]*Queue   // Handle read/write operations for each file
	MetadataReplicaConn map[string]net.Conn // connection with metadata replica nodes

	// A block is represented as "filename:version:blocknum"
	NodesToBlocks map[string][]string // Maps node ID to the blocks they are storing
	BlockToNodes  map[string][]string // Maps block to list of node IDs that store the block
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
	server.MetadataReplicaConn = make(map[string]net.Conn)

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

	Log.Infof("TCP File Server is listening on port %d...\n", server.Port)

	go server.pollTasks()
	go server.startRebalanceRoutine()

	for {
		conn, err := listener.Accept()
		if err != nil {
			// Log.Warnf("Error accepting connection: %s\n", err)
			continue
		}

		// Log.Debugf("Accepted connection from %s\n", conn.RemoteAddr())
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
	Log.Debug("node join: ", node)
	s.Nodes[node] = true
	s.NodesToBlocks[node] = []string{}
	s.Mutex.Unlock()

	s.addMetadataReplicaConnection(node)
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

	if found, ok := s.MetadataReplicaConn[node]; ok {
		found.Close()
		delete(s.MetadataReplicaConn, node)
	}
}

func (server *Server) DeleteFileAndBlocks(file File) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(file.Filename, file.Version, i)

		if replicas, ok := server.BlockToNodes[blockName]; ok {
			for _, replica := range replicas {
				server.NodesToBlocks[replica] = common.RemoveElement(server.NodesToBlocks[replica], blockName)
			}
		}

		if common.FileExists(server.Directory + "/" + blockName) {
			os.Remove(server.Directory + "/" + blockName)
		}

		delete(server.BlockToNodes, blockName)
	}

	if found, ok := server.Files[file.Filename]; ok && found.Version > file.Version {
		return
	}

	delete(server.Files, file.Filename)
	Log.Warn("File was deleted:", file.Filename)
}

func (s *Server) addMetadataReplicaConnection(node string) bool {
	if !s.IsMetadataReplicaNode(common.REPLICA_FACTOR, node) {
		return false
	}

	conn, err := net.Dial("tcp", node)
	if err != nil {
		Log.Warn("Failed to connect", node)
		return false
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// close old connection if there is one
	if found, ok := s.MetadataReplicaConn[node]; ok {
		found.Close()
	}

	s.MetadataReplicaConn[node] = conn

	// replicata all metdata
	for _, file := range s.Files {
		go replicateFileMetadata([]net.Conn{conn}, file)

		for i := 0; i < file.NumBlocks; i++ {
			blockName := common.GetBlockName(file.Filename, file.Version, i)
			go replicaBlockMetadata([]net.Conn{conn}, blockName, s.BlockToNodes[blockName])
		}
	}

	// NOTE: rebalance routine will replicate data blocks

	return true
}
