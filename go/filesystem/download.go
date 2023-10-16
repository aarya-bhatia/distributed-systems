package filesystem

import (
	"cs425/common"
	"fmt"
	"io"
	"math/rand"
	"net"
)

func (server *Server) DownloadBlockResponse(conn net.Conn, blockName string) {
	if block, ok := server.Storage[blockName]; ok {
		common.SendAll(conn, block.Data, block.Size)
	}
}

func (server *Server) DownloadBlockRequest(replicaID int, blockName string) []byte {
	buffer := make([]byte, common.BLOCK_SIZE)
	bufferSize := 0

	replicaInfo := server.Nodes[replicaID]

	replicaConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", replicaInfo.Hostname, replicaInfo.TCPPort))
	if err != nil {
		Log.Debug("Failed to establish connection", err)
		return nil
	}

	request := fmt.Sprintf("DOWNLOAD %s\n", blockName)
	if common.SendAll(replicaConn, []byte(request), len(request)) < 0 {
		return nil
	}
	for bufferSize < common.BLOCK_SIZE {
		n, err := replicaConn.Read(buffer[bufferSize:])
		if err == io.EOF || n == 0 {
			break
		} else if err != nil {
			Log.Warn(err)
			return nil
		}
		bufferSize += n
	}

	if bufferSize == 0 {
		return nil
	}

	Log.Debugf("Received %d bytes of block %s from replica %d\n", bufferSize, blockName, replicaID)
	return buffer[:bufferSize]
}

// Send file to client
func (server *Server) DownloadFile(conn net.Conn, filename string) {
	file, ok := server.Files[filename]

	if !ok {
		conn.Write([]byte("ERROR\nFile not found\n"))
		return
	}

	response := fmt.Sprintf("OK %s:%d:%d\n", filename, file.Version, file.FileSize)

	if common.SendAll(conn, []byte(response), len(response)) < 0 {
		return
	}

	bytesSent := 0

	Log.Debugf("To send %d blocks\n", file.NumBlocks)

	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(filename, file.Version, i)
		replicas := server.BlockToNodes[blockName]

		if len(replicas) == 0 {
			Log.Warnf("No replicas found for block %d\n", i)
			return
		}

		replicaID := replicas[rand.Intn(len(replicas))]
		replica := server.Nodes[replicaID]

		if replica.ID == server.Info.ID {
			block := server.Storage[blockName]
			if common.SendAll(conn, block.Data, block.Size) < 0 {
				Log.Warn("Failed to send block")
				return
			}

			bytesSent += block.Size
		} else {
			buffer := server.DownloadBlockRequest(replicaID, blockName)
			if buffer == nil {
				Log.Warn("Failed to download block")
				return
			}

			if common.SendAll(conn, buffer, len(buffer)) < 0 {
				Log.Warn("Failed to send block")
				return
			}
			bytesSent += len(buffer)
		}
	}

	Log.Infof("Sent file %s (%d bytes) to client %s\n", filename, bytesSent, conn.RemoteAddr())
}
