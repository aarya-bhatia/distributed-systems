package filesystem

import (
	"cs425/common"
	"fmt"
	"net"

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

func SendBlock(block string, data []byte, conn net.Conn) bool {
	if !common.SendMessage(conn, fmt.Sprintf("UPLOAD %s %d\n", block, len(data))) {
		return false
	}

	if !common.GetOKMessage(conn) {
		return false
	}

	if common.SendAll(conn, data, len(data)) < 0 {
		return false
	}

	if !common.GetOKMessage(conn) {
		return false
	}

	return true
}

func ReceiveBlock(block string, size int, conn net.Conn) ([]byte, bool) {
	request := fmt.Sprintf("DOWNLOAD %s\n", block)
	log.Debug(request)
	if !common.SendMessage(conn, request) {
		return nil, false
	}

	buffer := make([]byte, size)
	bufferSize := 0

	for bufferSize < size {
		n, err := conn.Read(buffer[bufferSize:])
		if err != nil {
			log.Warn(err)
			return nil, false
		}
		bufferSize += n
	}

	if bufferSize < size {
		log.Warn("Insufficient bytes:", block)
		return nil, false
	}

	log.Debugf("downloaded block %s with %d bytes from %s\n", block, bufferSize, conn.RemoteAddr())

	return buffer[:bufferSize], true
}

func DownloadBlock(block BlockMetadata, connCache *ConnectionCache) ([]byte, bool) {
	for _, replica := range common.Shuffle(block.Replicas) {
		node := common.GetNodeByID(replica)
		addr := common.GetAddress(node.Hostname, node.TCPPort)
		conn := connCache.GetConnection(addr)
		if conn == nil {
			continue
		}
		if data, ok := ReceiveBlock(block.Block, block.Size, conn); ok {
			return data, true
		}
	}

	return nil, false
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

		if !SendBlock(block.Block, data, conn) {
			return false
		}
	}

	return true
}
