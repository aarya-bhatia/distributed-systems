package common

import (
	"fmt"
	// "github.com/stretchr/testify/assert"
	"testing"
)

// type NodeInfo struct {
// 	ID       int
// 	Hostname string
// 	Port     int
// 	State    int
// }
//
// var nodes []*NodeInfo = []*NodeInfo{
// 	{ID: 1, Hostname: "localhost", Port: 5000, State: STATE_ALIVE},
// 	{ID: 2, Hostname: "localhost", Port: 5001, State: STATE_ALIVE},
// 	{ID: 3, Hostname: "localhost", Port: 5002, State: STATE_ALIVE},
// 	{ID: 4, Hostname: "localhost", Port: 5003, State: STATE_ALIVE},
// }

// func PrintAliveNodes() {
// 	aliveNodes := GetAliveNodes(nodes)
// 	fmt.Println("Nodes")
// 	for _, node := range aliveNodes {
// 		fmt.Println(node)
// 	}
// }
//
// func PrintMetadata(files []string, count int) {
// 	for _, filename := range files {
// 		hash := GetHash(filename)
// 		replicas := GetReplicaNodes(filename, count)
// 		fmt.Printf("filename=%s, hash=%d, replicas=", filename, hash)
// 		for _, replica := range replicas {
// 			fmt.Printf("%d ", replica.ID)
// 		}
// 		fmt.Printf("\n")
// 	}
// }

// func TestReplicas(t *testing.T) {
// 	assert.True(t, len(nodes) == 4)
//
// 	count := 2
// 	files := []string{}
//
// 	for i := 1; i < 10; i++ {
// 		filename := fmt.Sprintf("file%d.txt", i+1)
// 		files = append(files, filename)
// 	}
//
// 	PrintAliveNodes()
// 	PrintMetadata(files, count)
//
// 	fmt.Println("Crashing node 1")
// 	nodes[0].State = STATE_FAILED
// 	PrintAliveNodes()
// 	PrintMetadata(files, count)
//
// 	fmt.Println("Crashing node 3")
// 	nodes[2].State = STATE_FAILED
// 	PrintAliveNodes()
// 	PrintMetadata(files, count)
// }

func TestUtil(t *testing.T) {
	size := int64(1024 * 1024) // 1 MB
	fmt.Printf("size: %d, num blocks: %d\n", size, GetNumFileBlocks(size))
	size = 6400
	fmt.Printf("size: %d, num blocks: %d\n", size, GetNumFileBlocks(size))
	SplitFileIntoBlocks("data", "blocks")
}
