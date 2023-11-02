package filesystem

import (
	"cs425/common"
	"fmt"
	"net"
	log "github.com/sirupsen/logrus"
)

// Read a file block from disk and send it to client
func downloadBlock(directory string, client net.Conn, blockName string) bool {
	log.Debugf("Sending block %s to client %s", blockName, client.RemoteAddr())
	if buffer := common.ReadFile(directory, blockName); buffer != nil {
		// log.Debug("block size:", len(buffer))
		if common.SendAll(client, buffer, len(buffer)) > 0 {
			return true
		}
	}

	return false
}

// Receive a file block from client at node and write it to disk
func uploadBlock(directory string, client net.Conn, blockName string, blockSize int) bool {
	buffer := make([]byte, common.BLOCK_SIZE)
	bufferSize := 0

	if !common.SendMessage(client, "OK") {
		return false
	}

	for bufferSize < blockSize {
		numRead, err := client.Read(buffer[bufferSize:])
		if err != nil {
			log.Warn(err)
			return false
		}
		if numRead == 0 {
			break
		}
		bufferSize += numRead
	}

	if bufferSize < blockSize {
		log.Warnf("Insufficient bytes read (%d of %d)\n", bufferSize, blockSize)
		return false
	}

	log.Debugf("Received block %s (%d bytes) from client %s", blockName, blockSize, client.RemoteAddr())

	if !common.WriteFile(directory, blockName, buffer, blockSize) {
		log.Warnf("Failed to write block %s to disk\n", blockName)
		return false
	}

	// log.Infof("Added block %s to disk\n", blockName)
	return true
}

// Download a block from source node to replicate it at current node
func replicateBlock(directory string, blockName string, blockSize int, source string) bool {
	log.Debugf("To replicate block %s from host %s\n", blockName, source)
	repliaConn, err := net.Dial("tcp", source)
	if err != nil {
		return false
	}
	defer repliaConn.Close()

	if !common.SendMessage(repliaConn, fmt.Sprintf("DOWNLOAD %s\n", blockName)) {
		return false
	}

	buffer := make([]byte, common.BLOCK_SIZE)
	size := 0
	for size < blockSize {
		n, err := repliaConn.Read(buffer[size:])
		if err != nil {
			return false
		}
		if n == 0 {
			break
		}
		size += n
	}

	return common.WriteFile(directory, blockName, buffer, size)
}
