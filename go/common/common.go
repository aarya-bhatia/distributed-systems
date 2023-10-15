package common

import "time"

const (
	MAX_NODES       = 10
	BLOCK_SIZE      = 1024 * 1024
	MIN_BUFFER_SIZE = 1024
	REPLICA_FACTOR  = 1

	JOIN_RETRY_TIMEOUT = time.Second * 10

	NODES_PER_ROUND = 4 // Number of random peers to send gossip every round

	GOSSIP_PROTOCOL            = 0
	GOSPSIP_SUSPICION_PROTOCOL = 1

	NODE_ALIVE     = 0
	NODE_SUSPECTED = 1
	NODE_FAILED    = 2

	DEFAULT_TCP_PORT = 5000
	DEFAULT_UDP_PORT = 6000
)

type Node struct {
	ID       int
	Hostname string
	UDPPort  int
	TCPPort  int
}

type Notifier interface {
	HandleNodeJoin(node *Node)
	HandleNodeLeave(node *Node)
}

var ProdCluster = []Node{
	{1, "fa23-cs425-0701.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{2, "fa23-cs425-0702.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{3, "fa23-cs425-0703.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{4, "fa23-cs425-0704.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{5, "fa23-cs425-0705.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{6, "fa23-cs425-0706.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{7, "fa23-cs425-0707.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{8, "fa23-cs425-0708.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{9, "fa23-cs425-0709.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{10, "fa23-cs425-0710.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
}

var LocalCluster = []Node{
	{1, "localhost", 6001, 5001},
	{2, "localhost", 6002, 5002},
	{3, "localhost", 6003, 5003},
	{4, "localhost", 6004, 5004},
	{5, "localhost", 6005, 5005},
}

var Cluster []Node

func GetNode(id int) Node {
	return Cluster[id-1]
}

func GetNodeByAddress(hostname string, udpPort int) *Node {
	for _, node := range Cluster {
		if node.Hostname == hostname && node.UDPPort == udpPort {
			return &node
		}
	}
	return nil
}
