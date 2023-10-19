package filesystem

import (
	"cs425/common"
	"strings"
	"net"
	"fmt"
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
	if buffer := readBlockFromDisk(server.Directory, task.Name); buffer != nil {
		common.SendAll(task.Client, buffer, len(buffer))
	}
}

func (server *Server) sendDownloadFileMetadata(client net.Conn, filename string) bool {
	file, ok := server.Files[filename]
	if !ok {
		client.Write([]byte("ERROR\nFile not found\n"))
		return false
	}

	response := ""
	for i := 0; i < common.GetNumFileBlocks(int64(file.FileSize)); i++ {
		blockName := fmt.Sprintf("%s:%d:%d", filename, file.Version, i)
		response += fmt.Sprintf("%s %s\n", blockName, strings.Join(server.BlockToNodes[blockName], ","))
	}

	return common.SendAll(client, []byte(response), len(response)) == len(response)
}
