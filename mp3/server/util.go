package main

import (
	"hash"
	"hash/fnv"
	"math"
)

// Hash string s to an integer between 1 and N
func GetHash(s string) int {
	N := len(nodes)
	var fnvHash hash.Hash32 = fnv.New32a()
	fnvHash.Write([]byte(s))
	hashValue := fnvHash.Sum32()
	return 1 + int(hashValue%uint32(N))
}

func FindClosestReplicaIndex(arr []*NodeInfo, left int, right int, hashValue int) int {
	// base case: when there is only one element in the array
	if left == right {
		return left
	}

	// calculate the middle index
	mid := int((left + right) / 2)

	// recursively search the left half of the array
	leftClosest := FindClosestReplicaIndex(arr, left, mid, hashValue)

	// recursively search the right half of the array
	rightClosest := FindClosestReplicaIndex(arr, mid+1, right, hashValue)

	// compare the absolute differences of the closest elements in the left and right halves
	if math.Abs(float64(leftClosest-hashValue)) <= math.Abs(float64(rightClosest-hashValue)) {
		return leftClosest
	} else {
		return rightClosest
	}
}

// Returns slice of alive nodes
func GetAliveNodes(nodes []*NodeInfo) []*NodeInfo {
	res := []*NodeInfo{}
	for i, node := range nodes {
		if node.State == STATE_ALIVE {
			res = append(res, nodes[i])
		}
	}
	return res
}

// The first R nodes with the lowest ID are selected as metadata replicas.
func GetMetadataReplicaNodes(count int) []*NodeInfo {
	aliveNodes := GetAliveNodes(nodes)
	return aliveNodes[:min(count, len(aliveNodes))]
}

// The nearest node to the hash of the filename is selected as primary replica.
// The next R-1 successors are selected as backup replicas.
func GetReplicaNodes(filename string, count int) []*NodeInfo {
	aliveNodes := GetAliveNodes(nodes)
	if len(aliveNodes) < count {
		return aliveNodes
	}

	hash := GetHash(filename)
	primary := FindClosestReplicaIndex(aliveNodes, 0, len(aliveNodes)-1, hash)
	replicas := []*NodeInfo{}

	for i := 0; i < count; i++ {
		j := (primary + i) % len(aliveNodes)
		replicas = append(replicas, aliveNodes[j])
	}

	return replicas
}

// Returns the number blocks for a file of given size
func GetNumFileBlocks(fileSize int64) int {
	n := int(fileSize / MAX_BLOCK_SIZE)
	if fileSize%MAX_BLOCK_SIZE > 0 {
		n += 1
	}
	return n
}

