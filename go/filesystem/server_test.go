package filesystem

import (
	"cs425/common"
	"fmt"
	"testing"
	// "github.com/stretchr/testify/assert"
)

func TestReplicas(t *testing.T) {
	count := 2
	files := []string{}

	for i := 1; i < 10; i++ {
		filename := fmt.Sprintf("file%d.txt", i+1)
		files = append(files, filename)
		fmt.Printf("Hash for file %s: %d\n", filename, common.GetHash(filename, len(common.Cluster)))
	}

	s := NewServer(common.Cluster[0])

	s.Nodes[1] = common.Cluster[0]
	s.Nodes[2] = common.Cluster[1]
	s.Nodes[3] = common.Cluster[2]
	s.Nodes[4] = common.Cluster[3]
	s.Nodes[5] = common.Cluster[4]

	for _, file := range files {
		fmt.Printf("Replicas for file %s: %v\n", file, s.GetReplicaNodes(file, count))
	}

	fmt.Println("Crashing 2")
	delete(s.Nodes, 2)

	for _, file := range files {
		fmt.Printf("Replicas for file %s: %v\n", file, s.GetReplicaNodes(file, count))
	}

	fmt.Println("Crashing 5")
	delete(s.Nodes, 5)

	for _, file := range files {
		fmt.Printf("Replicas for file %s: %v\n", file, s.GetReplicaNodes(file, count))
	}
}
