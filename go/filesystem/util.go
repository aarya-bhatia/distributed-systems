package filesystem

import (
	"container/heap"
	"cs425/common"
	"cs425/priqueue"
	"fmt"
	"io"
	"os"
)

type FileEntry struct {
	Name string
	Size int64
}

// Node with smallest ID
func (s *Server) GetLeaderNode(count int) int {

	pq := make(priqueue.PriorityQueue, 0)
	heap.Init(&pq)

	for ID := range s.Nodes {
		heap.Push(&pq, &priqueue.Item{Key: ID})
	}

	item := heap.Pop(&pq).(*priqueue.Item)
	return item.Key
}

// The first R nodes with the lowest ID are selected as metadata replicas.
func (s *Server) GetMetadataReplicaNodes(count int) []int {

	pq := make(priqueue.PriorityQueue, 0)
	heap.Init(&pq)

	for ID := range s.Nodes {
		heap.Push(&pq, &priqueue.Item{Key: ID})
	}

	res := []int{}
	for r := 0; r < count && pq.Len() > 0; r++ {
		item := heap.Pop(&pq).(*priqueue.Item)
		res = append(res, item.Key)
	}

	return res
}

// Get the `count` nearest nodes to the file hash
func (s *Server) GetReplicaNodes(filename string, count int) []int {

	fileHash := common.GetHash(filename, len(common.Cluster))
	pq := make(priqueue.PriorityQueue, 0)
	heap.Init(&pq)

	for ID := range s.Nodes {
		distance := ID - fileHash
		if distance < 0 {
			distance = -distance
		}

		heap.Push(&pq, &priqueue.Item{Key: distance, Value: ID})
	}

	res := []int{}
	for r := 0; r < count && pq.Len() > 0; r++ {
		item := heap.Pop(&pq).(*priqueue.Item)
		res = append(res, item.Value)
	}

	return res
}

func writeBlockToDisk(directory string, blockName string, buffer []byte, blockSize int) bool {
	filename := fmt.Sprintf("%s/%s", directory, blockName)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		Log.Warn(err)
		return false
	}
	defer file.Close()

	_, err = file.Write(buffer[:blockSize])
	if err != nil {
		Log.Warn(err)
		return false
	}

	return true
}

func readBlockFromDisk(directory string, blockName string) []byte {
	filename := fmt.Sprintf("%s/%s", directory, blockName)
	file, err := os.Open(filename)
	if err != nil {
		Log.Warn(err)
		return nil
	}

	buffer, err := io.ReadAll(file)
	if err != nil {
		Log.Warn(err)
		return nil
	}

	return buffer
}

func getFilesInDirectory(directoryPath string) ([]FileEntry, error) {
	var files []FileEntry

	dir, err := os.Open(directoryPath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	dirEntries, err := dir.Readdir(0)
	if err != nil {
		return nil, err
	}

	for _, entry := range dirEntries {
		if !entry.IsDir() {
			files = append(files, FileEntry{entry.Name(), entry.Size()})
		}
	}

	return files, nil
}
