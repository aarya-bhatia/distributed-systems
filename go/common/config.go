package common

import (
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"
)

const (
	MAX_NODES       = 10
	BLOCK_SIZE      = 8 * 1024 * 1024
	MIN_BUFFER_SIZE = 1024
	REPLICA_FACTOR  = 4

	POLL_INTERVAL               = 10 * time.Millisecond
	REBALANCE_INTERVAL          = 5 * time.Second
	METADATA_REBALANCE_INTERVAL = 5 * time.Second

	CLIENT_HEARTBEAT_INTERVAL = time.Second
	CLIENT_TIMEOUT            = 5 * time.Second

	JOIN_RETRY_TIMEOUT = time.Second * 5

	UPLOAD_RETRY_TIME   = 2 * time.Second
	DOWNLOAD_RETRY_TIME = 2 * time.Second

	MAX_DOWNLOAD_RETRIES = 5
	MAX_UPLOAD_RETRIES   = 5

	NODES_PER_ROUND = 4 // Number of random peers to send gossip every round

	GOSSIP_PROTOCOL            = 0
	GOSPSIP_SUSPICION_PROTOCOL = 1

	FILE_TRUNCATE = 0
	FILE_APPEND   = 1

	NODE_ALIVE     = 0
	NODE_SUSPECTED = 1
	NODE_FAILED    = 2

	SHELL_PORT = 3000

	FD_PORT             = 4000
	SDFS_RPC_PORT       = 5000
	MAPLEJUICE_RPC_PORT = 7000

	MAPLE_CHUNK_LINE_COUNT = 100
	MAPLE_JUICE_LEADER_ID  = 1

	local_introducer_host = "localhost"
	local_introducer_port = 34000
	prod_introducer_host  = "fa23-cs425-0701.cs.illinois.edu"
	prod_introducer_port  = 34000
)

var INTRODUCER_ADDRESS string

type Node struct {
	ID                int
	Hostname          string
	UDPPort           int
	SDFSRPCPort       int
	MapleJuiceRPCPort int
}

var ProdCluster = []Node{
	{1, "fa23-cs425-0701.cs.illinois.edu", FD_PORT, SDFS_RPC_PORT, MAPLEJUICE_RPC_PORT},
	{2, "fa23-cs425-0702.cs.illinois.edu", FD_PORT, SDFS_RPC_PORT, MAPLEJUICE_RPC_PORT},
	{3, "fa23-cs425-0703.cs.illinois.edu", FD_PORT, SDFS_RPC_PORT, MAPLEJUICE_RPC_PORT},
	{4, "fa23-cs425-0704.cs.illinois.edu", FD_PORT, SDFS_RPC_PORT, MAPLEJUICE_RPC_PORT},
	{5, "fa23-cs425-0705.cs.illinois.edu", FD_PORT, SDFS_RPC_PORT, MAPLEJUICE_RPC_PORT},
	{6, "fa23-cs425-0706.cs.illinois.edu", FD_PORT, SDFS_RPC_PORT, MAPLEJUICE_RPC_PORT},
	{7, "fa23-cs425-0707.cs.illinois.edu", FD_PORT, SDFS_RPC_PORT, MAPLEJUICE_RPC_PORT},
	{8, "fa23-cs425-0708.cs.illinois.edu", FD_PORT, SDFS_RPC_PORT, MAPLEJUICE_RPC_PORT},
	{9, "fa23-cs425-0709.cs.illinois.edu", FD_PORT, SDFS_RPC_PORT, MAPLEJUICE_RPC_PORT},
	{10, "fa23-cs425-0710.cs.illinois.edu", FD_PORT, SDFS_RPC_PORT, MAPLEJUICE_RPC_PORT},
}

var LocalCluster = []Node{
	{1, "localhost", FD_PORT + 1, SDFS_RPC_PORT + 1, MAPLEJUICE_RPC_PORT + 1},
	{2, "localhost", FD_PORT + 2, SDFS_RPC_PORT + 2, MAPLEJUICE_RPC_PORT + 2},
	{3, "localhost", FD_PORT + 3, SDFS_RPC_PORT + 3, MAPLEJUICE_RPC_PORT + 3},
	{4, "localhost", FD_PORT + 4, SDFS_RPC_PORT + 4, MAPLEJUICE_RPC_PORT + 4},
	{5, "localhost", FD_PORT + 5, SDFS_RPC_PORT + 5, MAPLEJUICE_RPC_PORT + 5},
	{6, "localhost", FD_PORT + 6, SDFS_RPC_PORT + 6, MAPLEJUICE_RPC_PORT + 6},
	{7, "localhost", FD_PORT + 7, SDFS_RPC_PORT + 7, MAPLEJUICE_RPC_PORT + 7},
	{8, "localhost", FD_PORT + 8, SDFS_RPC_PORT + 8, MAPLEJUICE_RPC_PORT + 8},
	{9, "localhost", FD_PORT + 9, SDFS_RPC_PORT + 9, MAPLEJUICE_RPC_PORT + 9},
	{10, "localhost", FD_PORT + 10, SDFS_RPC_PORT + 10, MAPLEJUICE_RPC_PORT + 10},
}

var Cluster []Node

func Setup() {
	hostname, err := os.Hostname()
	if err != nil {
		os.Exit(1)
	}

	if os.Getenv("environment") == "production" || strings.Index(hostname, "illinois.edu") > 0 {
		os.Setenv("environment", "production")
		Cluster = ProdCluster
		INTRODUCER_ADDRESS = GetAddress(prod_introducer_host, prod_introducer_port)
	} else {
		os.Setenv("environment", "development")
		Cluster = LocalCluster
		INTRODUCER_ADDRESS = GetAddress(local_introducer_host, local_introducer_port)
	}

	logrus.Println("Environment:", os.Getenv("environment"))

	customFormatter := new(logrus.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	logrus.SetFormatter(customFormatter)

	logrus.SetReportCaller(false)
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetOutput(os.Stderr)
}

func GetCurrentNode() *Node {
	hostname, err := os.Hostname()
	if err != nil {
		os.Exit(1)
	}

	for _, node := range Cluster {
		if node.Hostname == hostname {
			return &node
		}
	}

	return nil
}

func GetNodeByID(ID int) *Node {
	for _, node := range Cluster {
		if node.ID == ID {
			return &node
		}
	}

	return nil
}

func GetNodeByAddress(hostname string, udpPort int) *Node {
	for _, node := range Cluster {
		if node.Hostname == hostname && node.UDPPort == udpPort {
			return &node
		}
	}

	return nil
}
