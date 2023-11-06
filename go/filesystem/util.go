package filesystem

import (
	"container/heap"
	"cs425/common"
	"cs425/priqueue"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
)

func GetNodeHash(node string) int {
	return common.GetHash(node, len(common.Cluster))
}

// First alive node in cluster, i.e. node with smallest ID
func (server *Server) GetLeaderNode() string {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	for _, node := range common.Cluster {
		ID := node.Hostname + ":" + fmt.Sprint(node.TCPPort)
		if _, ok := server.Nodes[ID]; ok {
			return ID
		}
	}
	return server.ID
}

// The first R alive nodes with the lowest ID are selected as metadata replicas.
func (s *Server) GetMetadataReplicaNodes(count int) []string {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	res := []string{}
	for _, node := range common.Cluster {
		ID := node.Hostname + ":" + fmt.Sprint(node.TCPPort)
		if _, ok := s.Nodes[ID]; ok {
			res = append(res, ID)
		}
	}
	if len(res) < count {
		return res
	}
	return res[:count]
}

func (s *Server) IsMetadataReplicaNode(count int, node string) bool {
	return common.HasElement(s.GetMetadataReplicaNodes(count), node)
}

func ConnectAll(nodes []string) []net.Conn {
	connections := []net.Conn{}

	for _, addr := range nodes {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			continue
		}
		connections = append(connections, conn)
	}

	return connections
}

func (s *Server) getMetadataReplicaConnections() []net.Conn {
	return ConnectAll(s.GetMetadataReplicaNodes(common.REPLICA_FACTOR))
}

// Get the `count` nearest nodes to the file hash
func GetReplicaNodes(nodes []string, filename string, count int) []string {
	fileHash := common.GetHash(filename, len(common.Cluster))
	pq := make(priqueue.PriorityQueue, 0)
	heap.Init(&pq)

	for _, node := range nodes {
		distance := GetNodeHash(node) - fileHash
		if distance < 0 {
			distance = -distance // abs value
		}
		heap.Push(&pq, &priqueue.Item{Key: distance, Value: node, TieKey: node})
	}

	res := []string{}
	for r := 0; r < count && pq.Len() > 0; r++ {
		item := heap.Pop(&pq).(*priqueue.Item)
		value := item.Value.(string)
		res = append(res, value)
	}

	return res
}

func (s *Server) GetAliveNodes() []string {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	res := []string{}
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
