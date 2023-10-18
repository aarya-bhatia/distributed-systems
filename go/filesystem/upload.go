package filesystem

import (
	"cs425/common"
	"fmt"
	"net"
)

func (server *Server) processUploadBlock(blockName string, buffer []byte, blockSize int) bool {
	blockData := make([]byte, blockSize)
	copy(blockData, buffer[:blockSize])

	replica := server.GetReplicaNodes(blockName, 1)[0]

	if replica == server.Info.ID {
		if !writeBlockToDisk(server.Directory, blockName, buffer, blockSize) {
			Log.Debugf("Added block %s to disk\n", blockName)
			return false
		}
	} else {
		replicaInfo := server.Nodes[replica]
		replicaConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", replicaInfo.Hostname, replicaInfo.TCPPort))
		if err != nil {
			Log.Debug("Failed to establish connection", err)
			return false
		}

		request := fmt.Sprintf("UPLOAD %s %d\n", blockName, blockSize)
		Log.Debug("Sending upload block request to node ", replica)
		if common.SendAll(replicaConn, []byte(request), len(request)) < 0 {
			return false
		}

		Log.Debug("Waiting for confirmation from node ", replica)
		if !common.GetOKMessage(replicaConn) {
			return false
		}

		Log.Debug("Sending block to node ", replica)
		if common.SendAll(replicaConn, buffer[:blockSize], blockSize) < 0 {
			return false
		}

		Log.Debugf("Sent block %s to node %v", blockName, replicaInfo)

		Log.Debug("Waiting for confirmation from node ", replica)
		if !common.GetOKMessage(replicaConn) {
			return false
		}

		replicaConn.Close()
	}

	server.NodesToBlocks[replica] = append(server.NodesToBlocks[replica], blockName)
	server.BlockToNodes[blockName] = append(server.BlockToNodes[blockName], replica)
	return true
}

func (server *Server) UploadFile(client net.Conn, filename string, filesize int, minAcks int) bool {
	version := 1
	if oldFile, ok := server.Files[filename]; ok {
		version = oldFile.Version + 1
	}

	client.Write([]byte("OK\n")) // Notify client to start uploading data

	numBlocks := common.GetNumFileBlocks(int64(filesize))
	Log.Debugf("To upload %d blocks\n", numBlocks)

	buffer := make([]byte, common.BLOCK_SIZE)
	bufferSize := 0
	bytesRead := 0
	blockCount := 0

	for bytesRead < filesize {
		numRead, err := client.Read(buffer[bufferSize:])
		if err != nil {
			Log.Debug(err)
			return false
		}

		if numRead == 0 {
			break
		}

		bufferSize += numRead

		if bufferSize == common.BLOCK_SIZE {
			Log.Debugf("Received block %d (%d bytes) from client %s", blockCount, bufferSize, client.RemoteAddr())
			blockName := common.GetBlockName(filename, version, blockCount)
			if !server.processUploadBlock(blockName, buffer, bufferSize) {
				Log.Warn("Failed to upload block")
				return false
			}
			bufferSize = 0
			blockCount += 1
		}

		bytesRead += numRead
	}

	if bufferSize > 0 {
		Log.Debugf("Received block %d (%d bytes) from client %s", blockCount, bufferSize, client.RemoteAddr())
		blockName := common.GetBlockName(filename, version, blockCount)
		if !server.processUploadBlock(blockName, buffer, bufferSize) {
			Log.Warn("Failed to upload block")
			return false
		}
	}

	if bytesRead < filesize {
		Log.Warnf("Insufficient bytes read (%d of %d)\n", bytesRead, filesize)
		return false
	}

	// TODO: Receive acks
	// for {
	// 	select {
	// 	case e := <-ackChannel:
	// 	case e := <-failureDetectorChannel:
	// 	}
	// }

	server.Files[filename] = &File{Filename: filename, Version: version, FileSize: filesize, NumBlocks: numBlocks}
	client.Write([]byte("OK\n"))
	return true
}

func (server *Server) UploadBlock(client net.Conn, filename string, version int, blockNum int, blockSize int) bool {
	// Notify client to start uploading data
	if common.SendAll(client, []byte("OK\n"), 3) < 0 {
		return false
	}

	buffer := make([]byte, common.BLOCK_SIZE)
	bufferSize := 0

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

	// Notify client of successful upload
	if common.SendAll(client, []byte("OK\n"), 3) < 0 {
		return false
	}

	Log.Debugf("Received block %d (%d bytes) from client %s", blockNum, blockSize, client.RemoteAddr())

	blockName := common.GetBlockName(filename, version, blockNum)

	if !writeBlockToDisk(server.Directory, blockName, buffer, blockSize) {
		Log.Debugf("Added block %s to disk\n", blockName)
		return false
	}

	server.NodesToBlocks[server.Info.ID] = append(server.NodesToBlocks[server.Info.ID], blockName)
	server.BlockToNodes[blockName] = append(server.BlockToNodes[blockName], server.Info.ID)

	return true
}
