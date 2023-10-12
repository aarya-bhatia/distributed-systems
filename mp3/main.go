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

var nodes []NodeInfo = []NodeInfo{
	{ID: 1, Hostname: "localhost", Port: 5000, State: STATE_ALIVE},
	{ID: 2, Hostname: "localhost", Port: 5001, State: STATE_ALIVE},
	{ID: 3, Hostname: "localhost", Port: 5002, State: STATE_ALIVE},
	{ID: 4, Hostname: "localhost", Port: 5003, State: STATE_ALIVE},
}

var files map[string]File
var queue []Request
var blocks map[string]BlockData

var fnvHash hash.Hash32 = fnv.New32a()

// Hash string s to an integer between 1 and N
func HashStringToInt(s string, N int) int {
	// Write the string to the hash object
	fnvHash.Write([]byte(s))

	// Get the hash sum as an unsigned 32-bit integer
	hashValue := fnvHash.Sum32()

	// Map the hash value to a number between 1 and N
	return int(hashValue%uint32(N)) + 1
}

// The first R nodes with the lowest ID are selected as metadata replicas.
func GetMetadataReplicaNodes() {
}

// The hash of the filename is selected as primary replica. The next R-1 successors are selected as backup replicas.
func GetReplicaNodes(filename string) {
}

func main() {
	fmt.Println("Hello world")
	// fmt.Println("Hash for 'file1.txt':", HashStringToInt("file1.txt", len(nodes)))
	// fmt.Println("Hash for 'file2.txt':", HashStringToInt("file2.txt", len(nodes)))
	// fmt.Println("Hash for 'file3.txt':", HashStringToInt("file3.txt", len(nodes)))
	// fmt.Println("Hash for 'file4.txt':", HashStringToInt("file4.txt", len(nodes)))
	// fmt.Println("Hash for 'file5.txt':", HashStringToInt("file5.txt", len(nodes)))
}
