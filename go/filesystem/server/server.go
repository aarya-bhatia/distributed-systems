package server

import (
	"container/heap"
	"cs425/common"
	"cs425/filesystem"
	"cs425/priqueue"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
	"os"
	"sync"
)

type Server struct {
	Hostname  string
	Port      int
	ID        int
	Directory string                     // Path to save blocks on disk
	Files     map[string]filesystem.File // Files stored by system
	Nodes     map[int]common.Node        // Set of alive nodes
	Mutex     sync.Mutex

	// A block is represented as "filename:version:blocknum"
	NodesToBlocks map[int][]string // Maps node ID to the blocks they are storing
	BlockToNodes  map[string][]int // Maps block to list of node IDs that store the block

	ResourceManager *ResourceManager
}

func NewServer(info common.Node, dbDirectory string) *Server {
	server := new(Server)
	server.Hostname = info.Hostname
	server.Port = info.RPCPort
	server.ID = info.ID
	server.Directory = dbDirectory
	server.Files = make(map[string]filesystem.File)
	server.NodesToBlocks = make(map[int][]string)
	server.BlockToNodes = make(map[string][]int)
	server.Nodes = make(map[int]common.Node)
	server.Nodes[server.ID] = info
	server.NodesToBlocks[server.ID] = []string{}
	server.ResourceManager = NewResourceManager()

	return server
}

func (server *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Port))
	if err != nil {
		log.Fatal(err)
	}
	if err := rpc.Register(server); err != nil {
		log.Fatal(err)
	}

	log.Info("SDFS master server is listening at", listener.Addr())

	go server.ResourceManager.StartTaskPolling()
	go server.ResourceManager.StartHeartbeatRoutine()

	go server.startRebalanceRoutine()
	go server.startMetadataRebalanceRoutine()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		// log.Debug("Connected:", conn.RemoteAddr())
		go rpc.ServeConn(conn)
	}
}

func GetAddressByID(id int) string {
	node := common.GetNodeByID(id)
	return common.GetAddress(node.Hostname, node.RPCPort)
}

func (s *Server) GetFiles() []filesystem.File {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	files := []filesystem.File{}
	for _, file := range s.Files {
		files = append(files, file)
	}
	return files
}

func GetNodeHash(node int) int {
	return node % len(common.Cluster)
}

// node with smallest ID
func (server *Server) GetLeaderNode() int {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	for _, node := range common.Cluster {
		if _, ok := server.Nodes[node.ID]; ok {
			return node.ID
		}
	}
	return server.ID
}

// The first R alive nodes with the lowest ID are selected as metadata replicas.
func (s *Server) GetMetadataReplicaNodes(count int) []int {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	res := []int{}
	for _, node := range common.Cluster {
		if node.ID == s.ID {
			continue
		}
		if _, ok := s.Nodes[node.ID]; ok {
			res = append(res, node.ID)
		}
	}
	if len(res) < count {
		return res
	}
	return res[:count]
}

// Get the `count` nearest nodes to the file hash
func GetReplicaNodes(nodes []int, filename string, count int) []int {
	fileHash := common.GetHash(filename, len(common.Cluster))
	pq := make(priqueue.PriorityQueue, 0)
	heap.Init(&pq)

	for _, node := range nodes {
		distance := common.Abs(GetNodeHash(node) - fileHash)
		heap.Push(&pq, &priqueue.Item{Key: distance, Value: node, TieKey: node})
	}

	res := []int{}
	for r := 0; r < count && pq.Len() > 0; r++ {
		item := heap.Pop(&pq).(*priqueue.Item)
		value := item.Value.(int)
		res = append(res, value)
	}

	return res
}

func (s *Server) GetAliveNodes() []int {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	res := []int{}
	for node := range s.Nodes {
		res = append(res, node)
	}
	return res
}

// Print file system metadata information to stdout
func (server *Server) PrintFileMetadata() {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"File", "Version", "Blocks", "Size"})

	for _, file := range server.Files {
		t.AppendRow(table.Row{
			file.Filename,
			file.Version,
			file.NumBlocks,
			file.FileSize,
		})
	}

	t.AppendSeparator()
	t.Render()
}

func (s *Server) HandleNodeJoin(info *common.Node) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	log.Debug("node join: ", *info)
	s.Nodes[info.ID] = *info
	s.NodesToBlocks[info.ID] = []string{}
}

func (s *Server) HandleNodeLeave(info *common.Node) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	log.Debug("node left: ", *info)
	for _, block := range s.NodesToBlocks[info.ID] {
		s.BlockToNodes[block] = common.RemoveElement(s.BlockToNodes[block], info.ID)
	}

	delete(s.NodesToBlocks, info.ID)
	delete(s.Nodes, info.ID)
}
