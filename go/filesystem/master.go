package filesystem

import (
	"bufio"
	"cs425/common"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// Periodically check for available tasks and launch thread for each task
func (server *Server) pollTasks() {
	for {
		server.Mutex.Lock()

		for _, queue := range server.FileQueues {
			task := queue.TryPop()
			if task == nil {
				continue
			}

			switch {
			case task.Action == UPLOAD_FILE:
				go server.uploadFileWrapper(task)
			case task.Action == DOWNLOAD_FILE:
				go server.downloadFileWrapper(task)
			case task.Action == DELETE_FILE:
				go server.deleteFileWrapper(task)
			default:
				Log.Warn("Invalid Action")
				task.Client.Close()
			}
		}

		server.Mutex.Unlock()
		time.Sleep(common.POLL_INTERVAL)
	}
}

func (server *Server) uploadFileWrapper(task *Request) {
	aliveNodes := server.GetAliveNodes()

	server.Mutex.Lock()

	newFile := File{
		Filename:  task.Name,
		FileSize:  task.Size,
		Version:   1,
		NumBlocks: common.GetNumFileBlocks(int64(task.Size)),
	}

	if oldFile, ok := server.Files[task.Name]; ok {
		newFile.Version = oldFile.Version + 1
	}

	server.Mutex.Unlock()

	blocks, ok := uploadFile(task.Client, newFile, aliveNodes)
	if !ok {
		server.getQueue(task.Name).WriteDone()
		return
	}

	server.Mutex.Lock()

	for block, replicas := range blocks {
		for _, replica := range replicas {
			server.BlockToNodes[block] = append(server.BlockToNodes[block], replica)
			server.NodesToBlocks[replica] = append(server.NodesToBlocks[replica], block)
		}
	}

	if oldFile, ok := server.Files[task.Name]; ok {
		go server.deleteFile(oldFile)
	}

	server.Files[task.Name] = newFile
	server.Mutex.Unlock()

	server.getQueue(task.Name).WriteDone()
	server.handleConnection(task.Client) // continue reading requests
}

func (server *Server) deleteFile(file File) {
	Log.Debug("To delete", file)

	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	deleteTasks := make(map[string][]string)

	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(file.Filename, file.Version, i)
		replicas, ok := server.BlockToNodes[blockName]
		if !ok {
			continue
		}
		for _, replica := range replicas {
			deleteTasks[replica] = append(deleteTasks[replica], blockName)
		}
	}

	for replica, blocks := range deleteTasks {
		go server.deleteBlocks(replica, blocks)
	}

	if found, ok := server.Files[file.Filename]; ok {
		if found.Version == file.Version {
			delete(server.Files, file.Filename)
			Log.Info("Deleted file", file.Filename)
		} else {
			Log.Debug("Skip delete as file is outdated")
		}
	}
}

func (server *Server) deleteFileWrapper(task *Request) {
	defer server.handleConnection(task.Client) // continue reading requests
	defer server.getQueue(task.Name).WriteDone()
	defer common.SendMessage(task.Client, "OK")

	server.Mutex.Lock()
	file, ok := server.Files[task.Name]
	if !ok {
		Log.Info("File not found", task.Name)
		return
	}
	server.Mutex.Unlock()

	server.deleteFile(file)
}

func (server *Server) deleteBlocks(replica string, blocks []string) {
	Log.Debugf("To delete %d block at node %s", len(blocks), replica)
	blocksDeleted := []string{}

	defer func() {
		server.Mutex.Lock()

		for _, block := range blocksDeleted {
			server.BlockToNodes[block] = common.RemoveElement(server.BlockToNodes[block], replica)
			server.NodesToBlocks[replica] = common.RemoveElement(server.NodesToBlocks[replica], block)
		}

		server.Mutex.Unlock()
	}()

	if replica == server.ID {
		for _, block := range blocks {
			if os.Remove(server.Directory+"/"+block) == nil {
				Log.Debug("Deleted block", block)
				blocksDeleted = append(blocksDeleted, block)
			} else {
				Log.Warn("Failed to delete block", block)
			}
		}
		return
	}

	conn, err := net.Dial("tcp", replica)
	if err != nil {
		return
	}

	buffer := make([]byte, common.MIN_BUFFER_SIZE)

	for _, block := range blocks {
		if !common.SendMessage(conn, fmt.Sprintf("DELETE %s", block)) {
			return
		}

		n, err := conn.Read(buffer)
		if err != nil {
			return
		}

		line := string(buffer[:n-1])
		tokens := strings.Split(line, " ")
		if len(tokens) < 2 || tokens[0] != "DELETE_OK" {
			break
		}

		Log.Debugf("Block %s deleted at node %s", tokens[1], replica)
		blocksDeleted = append(blocksDeleted, tokens[1])
	}
}

// Send replica list to client, wait for client to finish upload, update metdata with client's replica list
// Returns local blockToNodes list for current upload
func uploadFile(client net.Conn, newFile File, aliveNodes []string) (map[string][]string, bool) {
	if !common.SendMessage(client, "OK") {
		return nil, false
	}

	blocks := make(map[string][]string)

	// send client replica list for all blocks to upload
	for i := 0; i < newFile.NumBlocks; i++ {
		blockName := fmt.Sprintf("%s:%d:%d", newFile.Filename, newFile.Version, i)
		replicas := GetReplicaNodes(aliveNodes, blockName, common.REPLICA_FACTOR)
		replicaEncoding := strings.Join(replicas, ",")
		line := fmt.Sprintf("%s %s\n", blockName, replicaEncoding)
		if common.SendAll(client, []byte(line), len(line)) < 0 {
			return nil, false
		}
	}

	if !common.SendMessage(client, "END") {
		return nil, false
	}

	reader := bufio.NewReader(client)

	// receive confirmation from client for each uploaded block
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			Log.Warn(err)
			return nil, false
		}

		line = line[:len(line)-1]
		Log.Debug("Received:", line)
		tokens := strings.Split(line, " ")

		if tokens[0] == "END" {
			break
		}

		if len(tokens) != 2 {
			Log.Warn("illegal response")
			return nil, false
		}

		blockName := tokens[0]
		replicas := tokens[1]

		for _, replica := range strings.Split(replicas, ",") {
			blocks[blockName] = append(blocks[blockName], replica)
		}
	}

	if len(blocks) != newFile.NumBlocks {
		Log.Warnf("insufficient blocks confirmed: %d out of %d", len(blocks), newFile.NumBlocks)
		return nil, false
	}

	Log.Infof("%s: Uploaded file %s\n", client.RemoteAddr(), newFile.Filename)
	return blocks, true
}

func (server *Server) downloadFileWrapper(task *Request) {
	server.Mutex.Lock()
	file, ok := server.Files[task.Name]
	server.Mutex.Unlock()

	if !ok {
		Log.Warn("Download failed: file not found")
		task.Client.Write([]byte("ERROR\nFile not found\n"))
		server.getQueue(task.Name).ReadDone()
		return
	}

	blocks := make(map[string][]string)
	server.Mutex.Lock()
	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(file.Filename, file.Version, i)
		blocks[blockName] = server.BlockToNodes[blockName]
	}
	server.Mutex.Unlock()

	Log.Debug("Starting download for file:", task.Name)
	downloadFile(task.Client, file, blocks)
	Log.Debug("Download finished for file:", task.Name)

	server.getQueue(task.Name).ReadDone()
	server.handleConnection(task.Client) // continue reading requests
}

// Send replica list to client, wait for client to finish download
func downloadFile(client net.Conn, file File, blocks map[string][]string) bool {
	Log.Debug("Sending client metadata for file", file.Filename)
	var blockSize int = common.BLOCK_SIZE
	for i := 0; i < file.NumBlocks; i++ {
		if i == file.NumBlocks-1 {
			blockSize = file.FileSize - (file.NumBlocks-1)*common.BLOCK_SIZE
		}
		blockName := fmt.Sprintf("%s:%d:%d", file.Filename, file.Version, i)
		replicas := strings.Join(blocks[blockName], ",")
		line := fmt.Sprintf("%s %d %s\n", blockName, blockSize, replicas)
		Log.Debug(line)
		if common.SendAll(client, []byte(line), len(line)) < 0 {
			Log.Warn("Download failed")
			return false
		}
	}

	client.Write([]byte("END\n"))
	Log.Debugf("Waiting for client %s to download %s\n", client.RemoteAddr(), file.Filename)
	client.Read(make([]byte, common.MIN_BUFFER_SIZE))
	Log.Infof("Client %s finished download for file %s\n", client.RemoteAddr(), file.Filename)
	return true
}
