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

func (m *ServerMetadata) UpdateBlockMetadata(block filesystem.BlockMetadata) {
	m.AddBlock(block.Block)
	for _, replica := range block.Replicas {
		m.AddNode(replica)
		m.NodesToBlocks[replica].Add(block.Block)
		m.BlockToNodes[block.Block].Add(replica)
	}
}

func (m *ServerMetadata) GetMetadata(file filesystem.File) filesystem.FileMetadata {
	res := filesystem.FileMetadata{File: file, Blocks: make([]filesystem.BlockMetadata, 0)}

	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(file.Filename, file.Version, i)
		blockSize := common.BLOCK_SIZE
		if i == file.NumBlocks-1 {
			blockSize = file.FileSize - (file.NumBlocks-1)*common.BLOCK_SIZE
		}

		blockMetadata := filesystem.BlockMetadata{
			Block:    blockName,
			Replicas: m.BlockToNodes[blockName].ToSlice(),
			Size:     blockSize,
		}

		res.Blocks = append(res.Blocks, blockMetadata)
	}

	return res
}

func (m *ServerMetadata) GetNewMetadata(file filesystem.File, aliveNodes []int) filesystem.FileMetadata {
	res := filesystem.FileMetadata{File: file, Blocks: make([]filesystem.BlockMetadata, 0)}

	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(file.Filename, file.Version, i)
		blockSize := common.BLOCK_SIZE
		if i == file.NumBlocks-1 {
			blockSize = file.FileSize - (file.NumBlocks-1)*common.BLOCK_SIZE
		}
		blockReplicas := GetReplicaNodes(aliveNodes, blockName, common.REPLICA_FACTOR)

		blockMetadata := filesystem.BlockMetadata{
			Block:    blockName,
			Replicas: blockReplicas,
			Size:     blockSize,
		}

		res.Blocks = append(res.Blocks, blockMetadata)
	}

	return res
}
