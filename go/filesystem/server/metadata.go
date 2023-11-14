package server

import (
	"cs425/common"
	"cs425/filesystem"

	set "github.com/deckarep/golang-set/v2"
)

type ServerMetadata struct {
	NodesToBlocks map[int]set.Set[string]
	BlockToNodes  map[string]set.Set[int]
}

func NewServerMetadata() *ServerMetadata {
	m := new(ServerMetadata)
	m.NodesToBlocks = make(map[int]set.Set[string])
	m.BlockToNodes = make(map[string]set.Set[int])
	return m
}

func (m *ServerMetadata) AddNode(node int) {
	if _, ok := m.NodesToBlocks[node]; !ok {
		m.NodesToBlocks[node] = set.NewSet[string]()
	}
}

func (m *ServerMetadata) RemoveNode(node int) {
	if _, ok := m.NodesToBlocks[node]; !ok {
		return
	}
	for block := range m.NodesToBlocks[node].Iter() {
		m.BlockToNodes[block].Remove(node)
	}
	delete(m.NodesToBlocks, node)
}

func (m *ServerMetadata) HasNode(node int) bool {
	_, ok := m.NodesToBlocks[node]
	return ok
}

func (m *ServerMetadata) HasBlock(block string) bool {
	_, ok := m.BlockToNodes[block]
	return ok
}

func (m *ServerMetadata) AddBlock(block string) {
	if _, ok := m.BlockToNodes[block]; !ok {
		m.BlockToNodes[block] = set.NewSet[int]()
	}
}

func (m *ServerMetadata) RemoveBlock(block string) {
	if _, ok := m.BlockToNodes[block]; !ok {
		return
	}
	for node := range m.BlockToNodes[block].Iter() {
		if blocks, ok := m.NodesToBlocks[node]; ok {
			blocks.Remove(block)
		}
	}
	delete(m.BlockToNodes, block)
}

func GetBlockSize(file filesystem.File, n int) int {
	if n > file.NumBlocks {
		return 0
	}

	if n == file.NumBlocks-1 {
		return file.FileSize - (file.NumBlocks-1)*common.BLOCK_SIZE
	}

	return common.BLOCK_SIZE
}

func (m *ServerMetadata) UpdateBlockMetadata(block filesystem.BlockMetadata) {
	m.AddBlock(block.Block)
	for _, replica := range block.Replicas {
		m.AddNode(replica)
		m.NodesToBlocks[replica].Add(block.Block)
		m.BlockToNodes[block.Block].Add(replica)
	}
}

// Computes the file metadata for an existing file
func (m *ServerMetadata) GetMetadata(file filesystem.File) filesystem.FileMetadata {
	res := filesystem.FileMetadata{File: file, Blocks: make([]filesystem.BlockMetadata, 0)}
	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(file.Filename, file.Version, i)
		blockReplicas := m.BlockToNodes[blockName].ToSlice()
		blockMetadata := filesystem.BlockMetadata{
			Block:    blockName,
			Replicas: blockReplicas,
			Size:     GetBlockSize(file, i),
		}
		res.Blocks = append(res.Blocks, blockMetadata)
	}
	return res
}

// Computes the file metadata for a new file
func (m *ServerMetadata) GetNewMetadata(file filesystem.File, aliveNodes map[int]common.Node) filesystem.FileMetadata {
	res := filesystem.FileMetadata{File: file, Blocks: make([]filesystem.BlockMetadata, 0)}
	aliveNodeSet := set.NewSetFromMapKeys[int](aliveNodes)
	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(file.Filename, file.Version, i)
		blockReplicas := GetReplicaNodes(aliveNodeSet.ToSlice(), blockName, common.REPLICA_FACTOR)

		blockMetadata := filesystem.BlockMetadata{
			Block:    blockName,
			Replicas: blockReplicas,
			Size:     0,
		}
		res.Blocks = append(res.Blocks, blockMetadata)
	}
	return res
}
