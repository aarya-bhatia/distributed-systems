package filesystem

import (
	"cs425/common"
	"fmt"
	log "github.com/sirupsen/logrus"
)

type File struct {
	Filename  string
	Version   int
	FileSize  int
	NumBlocks int
}

type BlockMetadata struct {
	Block    string
	Size     int
	Replicas []int
}

type FileMetadata struct {
	File   File
	Blocks []BlockMetadata
}

func DownloadBlock(block BlockMetadata, connCache *ConnectionCache) ([]byte, bool) {
	for _, replica := range common.Shuffle(block.Replicas) {
		if data, ok := TryDownloadBlock(block, replica, connCache); ok {
			return data, true
		}
	}
	return nil, false
}

func TryDownloadBlock(block BlockMetadata, replica int, connCache *ConnectionCache) ([]byte, bool) {
	node := common.GetNodeByID(replica)
	addr := common.GetAddress(node.Hostname, node.TCPPort)
	conn := connCache.GetConnection(addr)
	if conn == nil {
		return nil, false
	}
	request := fmt.Sprintf("DOWNLOAD %s\n", block.Block)
	log.Debug(request)
	if !common.SendMessage(conn, request) {
		return nil, false
	}

	buffer := make([]byte, block.Size)
	bufferSize := 0

	for bufferSize < block.Size {
		n, err := conn.Read(buffer[bufferSize:])
		if err != nil {
			log.Warn(err)
			return nil, false
		}
		bufferSize += n
	}

	if bufferSize < block.Size {
		log.Warn("Insufficient bytes:", block.Block)
		return nil, false
	}

	log.Debugf("downloaded block %s with %d bytes from %s\n", block.Block, bufferSize, addr)

	return buffer[:bufferSize], true
}

func UploadBlock(block BlockMetadata, data []byte, connCache *ConnectionCache) bool {
	for _, replica := range block.Replicas {
		node := common.GetNodeByID(replica)
		addr := common.GetAddress(node.Hostname, node.TCPPort)

		log.Debugf("Uploading block %s (%d bytes) to %s\n", block.Block, block.Size, addr)

		conn := connCache.GetConnection(addr)
		if conn == nil {
			return false
		}

		if !common.SendMessage(conn, fmt.Sprintf("UPLOAD %s %d\n", block.Block, block.Size)) {
			return false
		}

		if !common.GetOKMessage(conn) {
			return false
		}

		if common.SendAll(conn, data, block.Size) < 0 {
			return false
		}

		if !common.GetOKMessage(conn) {
			return false
		}
	}

	return true
}
