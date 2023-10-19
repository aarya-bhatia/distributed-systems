package filesystem

import (
	"bufio"
	"cs425/common"
	"fmt"
	"net"
	"strings"
)

func (server *Server) handleUploadBlockRequest(task *Request) {
	defer task.Client.Close()

	if server.UploadBlock(task.Client, task.Name, task.Size) {
		task.Client.Write([]byte("OK\n"))
	} else {
		task.Client.Write([]byte("ERROR\n"))
	}
}

// Receive a file block from client at node and write it to disk
func (server *Server) UploadBlock(client net.Conn, blockName string, blockSize int) bool {
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

	if !writeBlockToDisk(server.Directory, blockName, buffer, blockSize) {
		Log.Debugf("Added block %s to disk\n", blockName)
		return false
	}

	return true
}

func (server *Server) finishUpload(client net.Conn, newFile File) {
	defer client.Close()
	defer server.getQueue(newFile.Filename).Done()

	reader := bufio.NewReader(client)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			Log.Warn(err)
			break
		}

		line = line[:len(line)-1]
		Log.Debug("Received:", line)
		tokens := strings.Split(line, " ")
		if len(tokens) != 2 {
			continue
		}

		blockName := tokens[0]
		replicas := tokens[1]

		for _, replica := range strings.Split(replicas, ",") {
			server.BlockToNodes[blockName] = append(server.BlockToNodes[blockName], replica)
			server.NodesToBlocks[replica] = append(server.NodesToBlocks[replica], blockName)
		}
	}

	Log.Infof("%s: Uploaded file %s\n", client.RemoteAddr(), newFile.Filename)
	server.Files[newFile.Filename] = newFile
}

func (server *Server) sendUploadFileMetadata(client net.Conn, filename string, filesize int64) bool {
	file, ok := server.Files[filename]
	newVersion := 1
	if ok {
		newVersion = file.Version + 1
	}

	for i := 0; i < common.GetNumFileBlocks(filesize); i++ {
		blockName := fmt.Sprintf("%s:%d:%d", filename, newVersion, i)
		line := fmt.Sprintf("%s %s\n", blockName, strings.Join(server.GetReplicaNodes(blockName, common.REPLICA_FACTOR), ","))
		if common.SendAll(client, []byte(line), len(line)) < 0 {
			return false
		}
	}

	client.Write([]byte("END\n"))

	return true
}
