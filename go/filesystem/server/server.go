package server

import (
	"container/heap"
	"cs425/common"
	"cs425/priqueue"
	"fmt"
	set "github.com/deckarep/golang-set/v2"
	"github.com/jedib0t/go-pretty/v6/table"
	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
	"os"
	"sync"
)

type File struct {
	Filename  string
	FileSize  int
	NumBlocks int
}

type BlockMetadata struct {
	Block    string
	Size     int
	Replicas []int
}

type FileMetadata struct {
	File   File
	Blocks []BlockMetadata
}

type Server struct {
	Hostname        string
	Port            int
	ID              int
	Directory       string              // Path to save blocks on disk
	Files           map[string]File     // Files stored by system
	Nodes           map[int]common.Node // Set of alive nodes
	Mutex           sync.Mutex
	ResourceManager *ResourceManager
	Metadata        *ServerMetadata
}

func NewServer(info common.Node, dbDirectory string) *Server {
	server := new(Server)
	server.Hostname = info.Hostname
	server.Port = info.RPCPort
	server.ID = info.ID
	server.Directory = dbDirectory
	server.Files = make(map[string]File)
	server.Nodes = make(map[int]common.Node)
	server.Nodes[server.ID] = info
	server.ResourceManager = NewResourceManager()
	server.Metadata = NewServerMetadata()
	server.Metadata.AddNode(server.ID)

	return server
}

func (server *Server) Start() {
	listener, err := net.Listen("tcp4", fmt.Sprintf("%s:%d", server.Hostname, server.Port))
	if err != nil {
		log.Fatal(err)
	}
	if err := rpc.Register(server); err != nil {
		log.Fatal(err)
	}

	log.Info("SDFS master server is listening at ", listener.Addr())

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

// func GetAddressByID(id int) string {
// 	node := common.GetNodeByID(id, common.SDFSCluster)
// 	return common.GetAddress(node.Hostname, node.RPCPort)
// }

func (s *Server) GetFiles() []File {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	files := []File{}
	for _, file := range s.Files {
		files = append(files, file)
	}
	return files
}

func GetNodeHash(node int) int {
	return node % len(common.SDFSCluster)
}

// node with smallest ID
func (server *Server) GetLeaderNode() int {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	for _, node := range common.SDFSCluster {
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
	for _, node := range common.SDFSCluster {
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
	fileHash := common.GetHash(filename, len(common.SDFSCluster))
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

func (s *Server) HandleNodeJoin(info *common.Node) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	if info == nil {
		return
	}

	if !common.IsSDFSNode(*info) {
		return
	}

	log.Debug("node join: ", *info)
	s.Nodes[info.ID] = *info
	s.Metadata.AddNode(info.ID)
}

func (s *Server) HandleNodeLeave(info *common.Node) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	if info == nil {
		return
	}

	if !common.IsSDFSNode(*info) {
		return
	}

	log.Debug("node left: ", *info)
	s.Metadata.RemoveNode(info.ID)
	delete(s.Nodes, info.ID)

}
func (server *Server) PrintFiles() {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"File", "Blocks", "Size", "Replicas"})

	for _, file := range server.Files {
		replicas := set.NewSet[int]()
		for i := 0; i < file.NumBlocks; i++ {
			blockName := common.GetBlockName(file.Filename, i)
			replicas = replicas.Union(server.Metadata.BlockToNodes[blockName])
		}

		t.AppendRow(table.Row{
			file.Filename,
			file.NumBlocks,
			file.FileSize,
			replicas.ToSlice(),
		})
	}

	t.AppendSeparator()
	t.Render()
}
