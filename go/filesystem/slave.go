package filesystem

import (
	"cs425/common"
	"fmt"
	"net"
)

// Read a file block from disk and send it to client
func downloadBlock(directory string, client net.Conn, blockName string) bool {
	Log.Debugf("Sending block %s to client %s", blockName, client.RemoteAddr())
	if buffer := common.ReadFile(directory, blockName); buffer != nil {
		Log.Debug("block size:", len(buffer))
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

	_, err := client.Write([]byte("OK\n"))
	if err != nil {
		return false
	}

	for bufferSize < blockSize {
		numRead, err := client.Read(buffer[bufferSize:])
		if err != nil {
			Log.Warn(err)
			return false
		}
		if numRead == 0 {
			break
		}
		bufferSize += numRead
	}

	if bufferSize < blockSize {
		Log.Warnf("Insufficient bytes read (%d of %d)\n", bufferSize, blockSize)
		return false
	}

	Log.Debugf("Received block %s (%d bytes) from client %s", blockName, blockSize, client.RemoteAddr())

	if !common.WriteFile(directory, blockName, buffer, blockSize) {
		Log.Debugf("Added block %s to disk\n", blockName)
		return false
	}

	client.Write([]byte("OK\n"))
	return true
}

// Download a block from source node to replicate it at current node
func replicateBlock(directory string, blockName string, blockSize int, source string) bool {
	Log.Debugf("To replicate block %s from host %s\n", blockName, source)
	request := fmt.Sprintf("DOWNLOAD %s\n", blockName)
	conn, err := net.Dial("tcp", source)
	if err != nil {
		return false
	}

	defer conn.Close()

	_, err = conn.Write([]byte(request))
	if err != nil {
		return false
	}

	buffer := make([]byte, common.BLOCK_SIZE)
	size := 0
	for size < blockSize {
		n, err := conn.Read(buffer[size:])
		if err != nil {
			return false
		}
		if n == 0 {
			break
		}
		size += n
	}

	if !common.WriteFile(directory, blockName, buffer, size) {
		return false
	}

	return true
}
