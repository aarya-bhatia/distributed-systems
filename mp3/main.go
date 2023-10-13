package main

import (
	"fmt"
	"hash"
	"hash/fnv"
)

const ENV = "DEV"

const REQUEST_READ = 0
const REQUEST_WRITE = 1

const STATE_ALIVE = 0
const STATE_FAILED = 1

const DEFAULT_PORT = 5000

const REPLICA_FACTOR = 2

type BlockMetadata struct {
	Filename         string
	Version          int
	BlockID          int
	BlockOffset      int
	BlockSize        int
	PrimaryReplicaID int
	BackupReplicaIDs []int
}

type BlockData struct {
	Filename    string
	Version     int
	BlockID     int
	BlockOffset int
	BlockSize   int
	Data        []byte
}

type File struct {
	Filename  string
	CreatorID int
	Version   int
	FileSize  int
	Blocks    []BlockMetadata
}

type Request struct {
	RequestType int
	Action      int
	Filename    string
	ClientID    int
}

type NodeInfo struct {
	ID       int
	Hostname string
	Port     int
	State    int
}

var nodes []*NodeInfo = []*NodeInfo{
	{ID: 1, Hostname: "localhost", Port: 5000, State: STATE_ALIVE},
	{ID: 2, Hostname: "localhost", Port: 5001, State: STATE_ALIVE},
	{ID: 3, Hostname: "localhost", Port: 5002, State: STATE_ALIVE},
	{ID: 4, Hostname: "localhost", Port: 5003, State: STATE_ALIVE},
}

var files map[string]*File
var queue []*Request
var blocks map[string]*BlockData

var fnvHash hash.Hash32 = fnv.New32a()

func GetAliveNodes() []*NodeInfo {
	res := []*NodeInfo{}
	for i, node := range nodes {
		if node.State == STATE_ALIVE {
			res = append(res, nodes[i])
		}
	}
	return res
}

// Hash string s to an integer between 0 and N-1
func GetHash(s string, N int) int {
	fnvHash.Write([]byte(s))
	hashValue := fnvHash.Sum32()
	return int(hashValue % uint32(N))
}

// The first R nodes with the lowest ID are selected as metadata replicas.
func GetMetadataReplicaNodes(count int) []*NodeInfo {
	aliveNodes := GetAliveNodes()
	return aliveNodes[:min(count, len(aliveNodes))]
}

// The hash of the filename is selected as primary replica. The next R-1 successors are selected as backup replicas.
func GetReplicaNodes(filename string, count int) []*NodeInfo {
	aliveNodes := GetAliveNodes()
	if len(aliveNodes) < count {
		return aliveNodes
	}

	hash := GetHash(filename, len(aliveNodes))
	replicas := []*NodeInfo{}

	for i := 0; i < count; i++ {
		j := (hash + i) % len(aliveNodes)
		replicas = append(replicas, aliveNodes[j])
	}

	return replicas
}

func main() {
	fmt.Println("Hello world")

	for i := 0; i < 5; i++ {
		fmt.Printf("Hash for 'file%d.txt': %d\n", i, GetHash(fmt.Sprintf("file%d.txt", i), len(nodes)))
	}
}
