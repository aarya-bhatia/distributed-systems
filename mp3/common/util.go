package common

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hash"
	"hash/fnv"
	"math"
	"net"
	"os"
	"strings"
)

// Hash string s to an integer between 0 and N-1
func GetHash(s string, N int) int {
	var fnvHash hash.Hash32 = fnv.New32a()
	fnvHash.Write([]byte(s))
	hashValue := fnvHash.Sum32()
	return int(hashValue % uint32(N))
}

func FindClosestIntegerIndex(arr []int, left int, right int, hashValue int) int {
	// base case: when there is only one element in the array
	if left == right {
		return left
	}

	// calculate the middle index
	mid := int((left + right) / 2)

	// recursively search the left half of the array
	leftClosest := FindClosestIntegerIndex(arr, left, mid, hashValue)

	// recursively search the right half of the array
	rightClosest := FindClosestIntegerIndex(arr, mid+1, right, hashValue)

	// compare the absolute differences of the closest elements in the left and right halves
	if math.Abs(float64(leftClosest-hashValue)) <= math.Abs(float64(rightClosest-hashValue)) {
		return leftClosest
	} else {
		return rightClosest
	}
}

// // Returns slice of alive nodes
// func GetAliveNodes(nodes []*NodeInfo) []*NodeInfo {
// 	res := []*NodeInfo{}
// 	for i, node := range nodes {
// 		if node.State == STATE_ALIVE {
// 			res = append(res, nodes[i])
// 		}
// 	}
// 	return res
// }

// The first R nodes with the lowest ID are selected as metadata replicas.
// func GetMetadataReplicaNodes(count int) []*NodeInfo {
// 	aliveNodes := GetAliveNodes(nodes)
// 	return aliveNodes[:min(count, len(aliveNodes))]
// }

// The nearest node to the hash of the filename is selected as primary replica.
// The next R-1 successors are selected as backup replicas.
// func GetReplicaNodes(filename string, count int) []*NodeInfo {
// 	aliveNodes := GetAliveNodes(nodes)
// 	if len(aliveNodes) < count {
// 		return aliveNodes
// 	}
//
// 	hash := GetHash(filename)
// 	primary := FindClosestReplicaIndex(aliveNodes, 0, len(aliveNodes)-1, hash)
// 	replicas := []*NodeInfo{}
//
// 	for i := 0; i < count; i++ {
// 		j := (primary + i) % len(aliveNodes)
// 		replicas = append(replicas, aliveNodes[j])
// 	}
//
// 	return replicas
// }

// Returns the number blocks for a file of given size
func GetNumFileBlocks(fileSize int64) int {
	n := int(fileSize / BLOCK_SIZE)
	if fileSize%BLOCK_SIZE > 0 {
		n += 1
	}
	return n
}

func Connect(hostname string, port int) net.Conn {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", hostname, port))
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func GetOKMessage(server net.Conn) bool {
	buffer := make([]byte, MIN_BUFFER_SIZE)
	n, err := server.Read(buffer)
	if err != nil {
		log.Println(err)
		return false
	}

	message := string(buffer[:n])

	if strings.Index(message, "OK") != 0 {
		log.Warn(message)
		return false
	}

	return true
}

// Returns true if all bytes are uploaded to network
func SendAll(conn net.Conn, buffer []byte, count int) int {
	sent := 0
	n := -1

	for sent < count && n != 0 {
		n, err := conn.Write(buffer[sent:count])
		if err != nil {
			log.Println(err)
			return -1
		}
		sent += n
	}

	return sent
}

func SplitFileIntoBlocks(filename string, outputDirectory string) bool {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		log.Println(err)
		return false
	}

	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		log.Println(err)
		return false
	}

	fileSize := info.Size()
	numBlocks := GetNumFileBlocks(fileSize)
	buffer := make([]byte, BLOCK_SIZE)

	for i := 0; i < numBlocks; i++ {
		n, err := f.Read(buffer)
		if err != nil {
			log.Println(err)
			return false
		}

		outputFilename := fmt.Sprintf("%s/%s_block%d", outputDirectory, filename, i)
		outputFile, err := os.OpenFile(outputFilename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0640)
		if err != nil {
			log.Println(err)
			return false
		}

		_, err = outputFile.Write(buffer)
		if err != nil {
			return false
		}

		outputFile.Close()

		if n < BLOCK_SIZE {
			break
		}
	}

	return true
}
