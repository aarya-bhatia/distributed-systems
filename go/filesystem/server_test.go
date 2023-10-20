package filesystem

// import (
// 	"cs425/common"
// 	"fmt"
// 	"testing"
// )

// func TestReplicas(t *testing.T) {
// 	count := 2
// 	files := []string{}

// 	for i := 1; i < 10; i++ {
// 		filename := fmt.Sprintf("file%d.txt", i+1)
// 		files = append(files, filename)
// 		fmt.Printf("Hash for file %s: %d\n", filename, common.GetHash(filename, len(common.Cluster)))
// 	}

// 	s := NewServer(common.Cluster[0], "db")
// 	address := []string{}

// 	for _, node := range common.Cluster {
// 		addr := fmt.Sprintf("%s:%d", node.Hostname, node.TCPPort)
// 		fmt.Println(node, GetNodeHash(addr))
// 		s.Nodes[addr] = true
// 		address = append(address, addr)
// 	}

// 	for _, file := range files {
// 		fmt.Printf("Replicas for file %s: %v\n", file, s.GetReplicaNodes(file, count))
// 	}

// 	fmt.Println("Crashing node: ", address[0])
// 	delete(s.Nodes, address[0])
// 	for _, file := range files {
// 		fmt.Printf("Replicas for file %s: %v\n", file, s.GetReplicaNodes(file, count))
// 	}

// 	fmt.Println("Crashing node: ", address[1])
// 	delete(s.Nodes, address[1])
// 	for _, file := range files {
// 		fmt.Printf("Replicas for file %s: %v\n", file, s.GetReplicaNodes(file, count))
// 	}
// }
