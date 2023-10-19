package filesystem

import (
	"cs425/common"
	"fmt"
	"net"
	"strings"
)

func (server *Server) finishDownload(client net.Conn, filename string) {
	defer client.Close()
	defer server.getQueue(filename).Done()
	buffer := make([]byte, common.MIN_BUFFER_SIZE)
	_, err := client.Read(buffer)
	if err != nil {
		Log.Warn("Download failed: ", err)
		return
	}
	Log.Infof("%s: Downloaded file %s\n", client.RemoteAddr(), filename)
}

func (server *Server) handleDownloadBlockRequest(task *Request) {
	defer task.Client.Close()
	Log.Debugf("Sending block %s to client %s", task.Name, task.Client.RemoteAddr())
	if buffer := readBlockFromDisk(server.Directory, task.Name); buffer != nil {
		common.SendAll(task.Client, buffer, len(buffer))
	}
}

func (server *Server) sendDownloadFileMetadata(client net.Conn, filename string) bool {
	file, ok := server.Files[filename]
	if !ok {
		Log.Warn("Download failed for file: ", filename)
		client.Write([]byte("ERROR\nFile not found\n"))
		return false
	}

	client.Write([]byte("OK\n"))
	Log.Debug("Sending client metadata for file ", filename)

	for i := 0; i < common.GetNumFileBlocks(int64(file.FileSize)); i++ {
		blockName := fmt.Sprintf("%s:%d:%d", filename, file.Version, i)
		line := fmt.Sprintf("%s %s\n", blockName, strings.Join(server.BlockToNodes[blockName], ","))
		if common.SendAll(client, []byte(line), len(line)) < 0 {
			return false
		}
	}

	client.Write([]byte("END\n"))
	return true
}
