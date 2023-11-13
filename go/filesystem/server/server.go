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

	// go server.startRebalanceRoutine() // TODO

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		log.Debug("Connected:", conn.RemoteAddr())
		go rpc.ServeConn(conn)
	}
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

	go s.replicateAllMetadata()
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

	go s.replicateAllMetadata()
}

func (s *Server) replicateAllMetadata() {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	for _, file := range s.Files {
		s.Mutex.Unlock()
		fileMetadata := new(filesystem.FileMetadata)
		if err := s.GetFileMetadata(&file.Filename, fileMetadata); err == nil {
			go s.replicateMetadata(*fileMetadata)
		}
		s.Mutex.Lock()
	}
}

func GetAddressByID(id int) string {
	node := common.GetNodeByID(id)
	return common.GetAddress(node.Hostname, node.RPCPort)
}

func (s *Server) replicateMetadata(fileMetadata filesystem.FileMetadata) {
	for _, replica := range s.GetMetadataReplicaNodes(common.REPLICA_FACTOR - 1) {
		if client, err := rpc.Dial("tcp", GetAddressByID(replica)); err == nil {
			defer client.Close()
			client.Call("Server.SetFileMetadata", &fileMetadata, new(bool))
		}
	}
}

// // To periodically redistribute file blocks to replicas to maintain equal load
// func (s *Server) startRebalanceRoutine() {
// 	log.Debug("Starting rebalance routine")
// 	for {
// 		aliveNodes := s.GetAliveNodes()
// 		s.Mutex.Lock()
// 		replicaTasks := make(map[string][]string) // maps replica to list of requests to be sent
//
// 		for _, file := range s.Files {
// 			for i := 0; i < file.NumBlocks; i++ {
// 				block := common.GetBlockName(file.Filename, file.Version, i)
// 				nodes, ok := s.BlockToNodes[block]
// 				if !ok {
// 					continue
// 				}
//
// 				// TODO: Delete extra replicas
// 				// if len(nodes) >= common.REPLICA_FACTOR {
// 				// 	continue
// 				// }
//
// 				replicas := GetReplicaNodes(aliveNodes, block, common.REPLICA_FACTOR)
// 				if len(replicas) == 0 {
// 					continue
// 				}
//
// 				// log.Debugf("Replicas for block %s: %v", block, replicas)
//
// 				required := common.MakeSet(replicas)
// 				current := common.MakeSet(nodes)
//
// 				blockSize := common.BLOCK_SIZE
// 				if i == file.NumBlocks-1 && file.FileSize%common.BLOCK_SIZE != 0 {
// 					blockSize = file.FileSize % common.BLOCK_SIZE
// 				}
//
// 				for replica := range required {
// 					if _, ok := current[replica]; !ok {
// 						source := nodes[rand.Intn(len(nodes))]
// 						request := fmt.Sprintf("ADD_BLOCK %s %d %s\n", block, blockSize, source)
// 						replicaTasks[replica] = append(replicaTasks[replica], request)
// 					}
// 				}
// 			}
// 		}
//
// 		delete(replicaTasks, s.ID)
//
// 		// if len(replicaTasks) > 0 {
// 		// 	log.Debug("Rebalance tasks:", replicaTasks)
// 		// }
//
// 		s.Mutex.Unlock()
//
// 		for replica, tasks := range replicaTasks {
// 			// log.Infof("Sending %d replication tasks to node %s:%v", len(tasks), replica, tasks)
// 			log.Debugf("Sending %d replication tasks to node %s", len(tasks), replica)
// 			go s.sendRebalanceRequests(replica, tasks)
// 		}
//
// 		time.Sleep(common.REBALANCE_INTERVAL)
// 	}
// }
//
// // Send all ADD_BLOCK requests to given replica over single tcp connection
// // Update server block metadata on success
// func (s *Server) sendRebalanceRequests(replica string, requests []string) {
// 	conn, err := net.Dial("tcp", replica)
// 	if err != nil {
// 		return
// 	}
// 	defer conn.Close()
//
// 	for _, request := range requests {
// 		if !common.SendMessage(conn, request) {
// 			return
// 		}
//
// 		if !common.GetOKMessage(conn) {
// 			return
// 		}
//
// 		var replicas []string
// 		blockName := strings.Split(request, " ")[1]
// 		s.Mutex.Lock()
// 		s.BlockToNodes[blockName] = common.AddUniqueElement(s.BlockToNodes[blockName], replica)
// 		s.NodesToBlocks[replica] = common.AddUniqueElement(s.NodesToBlocks[replica], blockName)
// 		replicas = s.BlockToNodes[blockName]
// 		s.Mutex.Unlock()
//
// 		conn := s.getMetadataReplicaConnections()
// 		go replicateBlockMetadata(conn, blockName, replicas)
// 	}
// }
