package filesystem

import (
	"bufio"
	"cs425/common"
	"fmt"
	"net"
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
			if task.Action == UPLOAD_FILE {
				go server.uploadFileWrapper(task)
			} else if task.Action == DOWNLOAD_FILE {
				go server.downloadFileWrapper(task)
			} else {
				Log.Warn("Invalid Action")
				task.Client.Close()
			}
		}
		server.Mutex.Unlock()
		time.Sleep(common.POLL_INTERVAL)
	}
}

func (server *Server) uploadFileWrapper(task *Request) {
	defer task.Client.Close()
	defer server.getQueue(task.Name).Done()

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
		return
	}

	server.Mutex.Lock()

	for block, replicas := range blocks {
		for _, replica := range replicas {
			server.BlockToNodes[block] = append(server.BlockToNodes[block], replica)
			server.NodesToBlocks[replica] = append(server.NodesToBlocks[replica], block)
		}
	}

	server.Files[task.Name] = newFile
	server.Mutex.Unlock()
}

// Send replica list to client, wait for client to finish upload, update metdata with client's replica list
// Returns local blockToNodes list for current upload
func uploadFile(client net.Conn, newFile File, aliveNodes []string) (map[string][]string, bool) {
	blocks := make(map[string][]string)

	for i := 0; i < newFile.NumBlocks; i++ {
		blockName := fmt.Sprintf("%s:%d:%d", newFile.Filename, newFile.Version, i)
		line := fmt.Sprintf("%s %s\n", blockName,
			strings.Join(GetReplicaNodes(aliveNodes, blockName,
				common.REPLICA_FACTOR), ","))
		if common.SendAll(client, []byte(line), len(line)) < 0 {
			return nil, false
		}
	}

	client.Write([]byte("END\n"))

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
			break
		}

		blockName := tokens[0]
		replicas := tokens[1]
		for _, replica := range strings.Split(replicas, ",") {
			blocks[blockName] = append(blocks[blockName], replica)
		}
	}

	Log.Infof("%s: Uploaded file %s\n", client.RemoteAddr(), newFile.Filename)
	return blocks, true
}

func (server *Server) downloadFileWrapper(task *Request) {
	defer task.Client.Close()
	defer server.getQueue(task.Name).Done()

	server.Mutex.Lock()
	file, ok := server.Files[task.Name]
	server.Mutex.Unlock()

	if !ok {
		Log.Warn("Download failed for file: ", task.Name)
		// task.Client.Write([]byte("ERROR\nFile not found\n"))
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
}

// Send replica list to client, wait for client to finish download
func downloadFile(client net.Conn, file File, blocks map[string][]string) bool {
	Log.Debug("Sending client metadata for file", file.Filename)
	for i := 0; i < file.NumBlocks; i++ {
		blockName := fmt.Sprintf("%s:%d:%d", file.Filename, file.Version, i)
		line := fmt.Sprintf("%s %s\n", blockName, strings.Join(blocks[blockName], ","))
		if common.SendAll(client, []byte(line), len(line)) < 0 {
			Log.Warn("Download failed")
			return false
		}
	}
	client.Write([]byte("END\n"))

	buffer := make([]byte, common.MIN_BUFFER_SIZE)
	_, err := client.Read(buffer)
	if err != nil {
		Log.Warn("Download failed:", err)
		return false
	}

	Log.Infof("Client %s downloaded file %s\n", client.RemoteAddr(), file.Filename)
	return true
}
