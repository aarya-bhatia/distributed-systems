package common

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	MAX_NODES       = 10
	BLOCK_SIZE      = 16 * 1024 * 1024 // 16 MB
	MIN_BUFFER_SIZE = 1024
	REPLICA_FACTOR  = 4

	POLL_INTERVAL      = 200 * time.Millisecond
	REBALANCE_INTERVAL = 9 * time.Second

	CLIENT_HEARTBEAT_INTERVAL = time.Second
	CLIENT_TIMEOUT            = 5 * time.Second

	JOIN_RETRY_TIMEOUT = time.Second * 5

	UPLOAD_RETRY_TIME   = 2 * time.Second
	DOWNLOAD_RETRY_TIME = 2 * time.Second

	MAX_DOWNLOAD_RETRIES = 1
	MAX_UPLOAD_RETRIES   = 1

	NODES_PER_ROUND = 4 // Number of random peers to send gossip every round

	GOSSIP_PROTOCOL            = 0
	GOSPSIP_SUSPICION_PROTOCOL = 1

	FILE_TRUNCATE = 0
	FILE_APPEND   = 1

	NODE_ALIVE     = 0
	NODE_SUSPECTED = 1
	NODE_FAILED    = 2

	SHELL_PORT = 3000

	SDFS_FD_PORT  = 4000
	SDFS_RPC_PORT = 5000

	MAPLEJUICE_FD_PORT  = 9000
	MAPLEJUICE_RPC_PORT = 7000

	MAPLE_CHUNK_LINE_COUNT = 100

	local_introducer_host = "localhost"
	local_introducer_port = 34000
	prod_introducer_host  = "fa23-cs425-0701.cs.illinois.edu"
	prod_introducer_port  = 34000
)

var INTRODUCER_ADDRESS string

type Node struct {
	ID       int
	Hostname string
	UDPPort  int
	RPCPort  int
}

var SDFSProdCluster = []Node{
	{1, "fa23-cs425-0701.cs.illinois.edu", SDFS_FD_PORT, SDFS_RPC_PORT},
	{2, "fa23-cs425-0702.cs.illinois.edu", SDFS_FD_PORT, SDFS_RPC_PORT},
	{3, "fa23-cs425-0703.cs.illinois.edu", SDFS_FD_PORT, SDFS_RPC_PORT},
	{4, "fa23-cs425-0704.cs.illinois.edu", SDFS_FD_PORT, SDFS_RPC_PORT},
	{5, "fa23-cs425-0705.cs.illinois.edu", SDFS_FD_PORT, SDFS_RPC_PORT},
	{6, "fa23-cs425-0706.cs.illinois.edu", SDFS_FD_PORT, SDFS_RPC_PORT},
	{7, "fa23-cs425-0707.cs.illinois.edu", SDFS_FD_PORT, SDFS_RPC_PORT},
	{8, "fa23-cs425-0708.cs.illinois.edu", SDFS_FD_PORT, SDFS_RPC_PORT},
	{9, "fa23-cs425-0709.cs.illinois.edu", SDFS_FD_PORT, SDFS_RPC_PORT},
	{10, "fa23-cs425-0710.cs.illinois.edu", SDFS_FD_PORT, SDFS_RPC_PORT},
}

var SDFSLocalCluster = []Node{
	{1, "localhost", SDFS_FD_PORT + 1, SDFS_RPC_PORT + 1},
	{2, "localhost", SDFS_FD_PORT + 2, SDFS_RPC_PORT + 2},
	{3, "localhost", SDFS_FD_PORT + 3, SDFS_RPC_PORT + 3},
	{4, "localhost", SDFS_FD_PORT + 4, SDFS_RPC_PORT + 4},
	{5, "localhost", SDFS_FD_PORT + 5, SDFS_RPC_PORT + 5},
	{6, "localhost", SDFS_FD_PORT + 6, SDFS_RPC_PORT + 6},
	{7, "localhost", SDFS_FD_PORT + 7, SDFS_RPC_PORT + 7},
	{8, "localhost", SDFS_FD_PORT + 8, SDFS_RPC_PORT + 8},
	{9, "localhost", SDFS_FD_PORT + 9, SDFS_RPC_PORT + 9},
	{10, "localhost", SDFS_FD_PORT + 10, SDFS_RPC_PORT + 10},
}

var ProdMapleJuiceCluster = []Node{
	{1, "fa23-cs425-0701.cs.illinois.edu", MAPLEJUICE_FD_PORT, MAPLEJUICE_RPC_PORT},
	{2, "fa23-cs425-0702.cs.illinois.edu", MAPLEJUICE_FD_PORT, MAPLEJUICE_RPC_PORT},
	{3, "fa23-cs425-0703.cs.illinois.edu", MAPLEJUICE_FD_PORT, MAPLEJUICE_RPC_PORT},
	{4, "fa23-cs425-0704.cs.illinois.edu", MAPLEJUICE_FD_PORT, MAPLEJUICE_RPC_PORT},
	{5, "fa23-cs425-0705.cs.illinois.edu", MAPLEJUICE_FD_PORT, MAPLEJUICE_RPC_PORT},
	{6, "fa23-cs425-0706.cs.illinois.edu", MAPLEJUICE_FD_PORT, MAPLEJUICE_RPC_PORT},
	{7, "fa23-cs425-0707.cs.illinois.edu", MAPLEJUICE_FD_PORT, MAPLEJUICE_RPC_PORT},
	{8, "fa23-cs425-0708.cs.illinois.edu", MAPLEJUICE_FD_PORT, MAPLEJUICE_RPC_PORT},
	{9, "fa23-cs425-0709.cs.illinois.edu", MAPLEJUICE_FD_PORT, MAPLEJUICE_RPC_PORT},
	{10, "fa23-cs425-0710.cs.illinois.edu", MAPLEJUICE_FD_PORT, MAPLEJUICE_RPC_PORT},
}

var LocalMapleJuiceCluster = []Node{
	{1, "localhost", MAPLEJUICE_FD_PORT + 1, MAPLEJUICE_RPC_PORT + 1},
	{2, "localhost", MAPLEJUICE_FD_PORT + 2, MAPLEJUICE_RPC_PORT + 2},
	{3, "localhost", MAPLEJUICE_FD_PORT + 3, MAPLEJUICE_RPC_PORT + 3},
	{4, "localhost", MAPLEJUICE_FD_PORT + 4, MAPLEJUICE_RPC_PORT + 4},
	{5, "localhost", MAPLEJUICE_FD_PORT + 5, MAPLEJUICE_RPC_PORT + 5},
	{6, "localhost", MAPLEJUICE_FD_PORT + 6, MAPLEJUICE_RPC_PORT + 6},
	{7, "localhost", MAPLEJUICE_FD_PORT + 7, MAPLEJUICE_RPC_PORT + 7},
	{8, "localhost", MAPLEJUICE_FD_PORT + 8, MAPLEJUICE_RPC_PORT + 8},
	{9, "localhost", MAPLEJUICE_FD_PORT + 9, MAPLEJUICE_RPC_PORT + 9},
	{10, "localhost", MAPLEJUICE_FD_PORT + 10, MAPLEJUICE_RPC_PORT + 10},
}

var SDFSCluster []Node
var MapleJuiceCluster []Node

func Setup() {
	hostname, err := os.Hostname()
	if err != nil {
		os.Exit(1)
	}

	if os.Getenv("environment") == "production" || strings.Index(hostname, "illinois.edU") > 0 {
		os.Setenv("environment", "production")
		SDFSCluster = SDFSProdCluster
		MapleJuiceCluster = ProdMapleJuiceCluster
		INTRODUCER_ADDRESS = fmt.Sprintf("%s:%d", prod_introducer_host, prod_introducer_port)
	} else {
		os.Setenv("environment", "development")
		SDFSCluster = SDFSLocalCluster
		MapleJuiceCluster = LocalMapleJuiceCluster
		INTRODUCER_ADDRESS = fmt.Sprintf("%s:%d", local_introducer_host, local_introducer_port)
	}

	logrus.Println("SDFS cluster:", SDFSCluster)
	logrus.Println("MapleJuice cluster:", MapleJuiceCluster)
	logrus.Println("Introducer:", INTRODUCER_ADDRESS)

	customFormatter := new(logrus.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	logrus.SetFormatter(customFormatter)

	logrus.SetReportCaller(false)
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetOutput(os.Stderr)
}

func GetCurrentNode(cluster []Node) *Node {
	hostname, err := os.Hostname()
	if err != nil {
		os.Exit(1)
	}

	return GetNodeByHostname(hostname, cluster)
}

func GetNodeByHostname(hostname string, cluster []Node) *Node {
	for _, node := range cluster {
		if node.Hostname == hostname {
			return &node
		}
	}
	return nil
}

func GetNodeByID(ID int, cluster []Node) *Node {
	for _, node := range cluster {
		if node.ID == ID {
			return &node
		}
	}
	return nil
}

func GetNodeByAddress(hostname string, udpPort int) *Node {
	for _, node := range SDFSCluster {
		if node.Hostname == hostname && node.UDPPort == udpPort {
			return &node
		}
	}
	for _, node := range MapleJuiceCluster {
		if node.Hostname == hostname && node.UDPPort == udpPort {
			return &node
		}
	}
	return nil
}
