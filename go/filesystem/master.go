package filesystem

import (
	"cs425/common"
	"fmt"
	log "github.com/sirupsen/logrus"
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
				log.Warn("Invalid Action")
				task.Client.Close()
			}
		}

		server.Mutex.Unlock()
		time.Sleep(common.POLL_INTERVAL)
	}
}

func (server *Server) uploadFileWrapper(task *Request) {
	defer server.handleConnection(task.Client)   // continue reading requests ( this is run last )
	defer server.getQueue(task.Name).WriteDone() // signal write complete

	aliveNodes := server.GetAliveNodes()

	// connect to all metadata nodes except self
	metadataReplicaConnections := ConnectAll(common.RemoveElement(server.GetMetadataReplicaNodes(common.REPLICA_FACTOR), server.ID))
	defer common.CloseAll(metadataReplicaConnections)

	server.Mutex.Lock()
	// create metadata of new file
	newFile := File{
		Filename:  task.Name,
		FileSize:  task.Size,
		Version:   1,
		NumBlocks: common.GetNumFileBlocks(int64(task.Size)),
	}

	// set new file version if file already exists
	if oldFile, ok := server.Files[task.Name]; ok {
		newFile.Version = oldFile.Version + 1
	}
	server.Mutex.Unlock()

	// to send replica list to client and wait for upload to finish
	blocks, ok := uploadFile(task.Client, newFile, aliveNodes)
	if !ok {
		common.SendMessage(task.Client, "UPLOAD_ERROR")
		return
	}

	for block, replicas := range blocks {
		// update metdata for new block
		for _, replica := range replicas {
			server.Mutex.Lock()
			server.BlockToNodes[block] = common.AddUniqueElement(server.BlockToNodes[block], replica)
			server.NodesToBlocks[replica] = common.AddUniqueElement(server.NodesToBlocks[replica], block)
			server.Mutex.Unlock()
		}

		// replicate metadata for new blocks
		if !replicateBlockMetadata(metadataReplicaConnections, block, replicas) {
			common.SendMessage(task.Client, "UPLOAD_ERROR")
			return
		}
	}

	// replicate metadata for new file
	if !replicateFileMetadata(metadataReplicaConnections, newFile) {
		common.SendMessage(task.Client, "UPLOAD_ERROR")
		return
	}

	// delete old file in background
	if oldFile, ok := server.Files[task.Name]; ok && oldFile.Version < newFile.Version {
		go server.deleteFile(oldFile)
	}

	// set new file metdata
	server.Mutex.Lock()
	server.Files[task.Name] = newFile
	server.Mutex.Unlock()

	// upload complete
	common.SendMessage(task.Client, "UPLOAD_OK")
}

func (server *Server) deleteFile(file File) {
	message := fmt.Sprintf("DELETE %s %d %d\n", file.Filename, file.Version, file.NumBlocks)

	// Send delete message to all nodes
	nodes := server.GetAliveNodes()
	for _, node := range nodes {
		go func(addr string) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			if common.SendMessage(conn, message) {
				if common.GetOKMessage(conn) {
					log.Debugf("File %s deleted from node %s", file.Filename, addr)
				}
			}
		}(node)
	}

	server.deleteFileLocal(file)
}

func (server *Server) deleteFileLocal(file File) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(file.Filename, file.Version, i)

		for _, replica := range server.BlockToNodes[blockName] {
			server.NodesToBlocks[replica] = common.RemoveElement(server.NodesToBlocks[replica], blockName)
		}

		if common.FileExists(server.Directory + "/" + blockName) {
			os.Remove(server.Directory + "/" + blockName)
		}

		delete(server.BlockToNodes, blockName)
	}

	if found, ok := server.Files[file.Filename]; ok && found.Version > file.Version {
		return
	}

	delete(server.Files, file.Filename)
	log.Warn("File deleted:", file.Filename)
}

func (server *Server) deleteFileWrapper(task *Request) {
	defer server.handleConnection(task.Client) // continue reading requests
	defer server.getQueue(task.Name).WriteDone()
	defer common.SendMessage(task.Client, "OK")

	server.Mutex.Lock()
	file, ok := server.Files[task.Name]
	server.Mutex.Unlock()

	// file not exist
	if !ok {
		return
	}

	server.deleteFile(file)
}

// Send replica list to client, wait for client to finish upload, update metdata with client's replica list
// Returns local blockToNodes list for current upload
func uploadFile(client net.Conn, newFile File, aliveNodes []string) (map[string][]string, bool) {
	if !common.SendMessage(client, "OK") {
		return nil, false
	}

	// store the required replicas for each block
	blocks := make(map[string][]string)

	// send client replica list for each block
	for i := 0; i < newFile.NumBlocks; i++ {
		blockName := common.GetBlockName(newFile.Filename, newFile.Version, i)
		replicas := GetReplicaNodes(aliveNodes, blockName, common.REPLICA_FACTOR)
		replicaEncoding := strings.Join(replicas, ",")
		blocks[blockName] = replicas
		if !common.SendMessage(client, blockName+" "+replicaEncoding) {
			return nil, false
		}
	}

	if !common.SendMessage(client, "END") {
		return nil, false
	}

	if !common.GetOKMessage(client) {
		return nil, false
	}

	return blocks, true
}

func (server *Server) downloadFileWrapper(task *Request) {
	defer server.handleConnection(task.Client) // continue reading requests
	defer server.getQueue(task.Name).ReadDone()

	server.Mutex.Lock()
	file, ok := server.Files[task.Name]
	server.Mutex.Unlock()

	if !ok {
		log.Warn("Download failed: file not found")
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

	log.Debug("Starting download:", task.Name)
	downloadFile(task.Client, file, blocks)
	log.Debug("Download finished:", task.Name)
}

// Send replica list to client, wait for client to finish download
func downloadFile(client net.Conn, file File, blocks map[string][]string) bool {
	var blockSize int = common.BLOCK_SIZE
	for i := 0; i < file.NumBlocks; i++ {
		if i == file.NumBlocks-1 {
			blockSize = file.FileSize - (file.NumBlocks-1)*common.BLOCK_SIZE
		}
		blockName := common.GetBlockName(file.Filename, file.Version, i)
		replicas := strings.Join(blocks[blockName], ",")
		line := fmt.Sprintf("%s %d %s\n", blockName, blockSize, replicas)
		log.Debug(line)
		if !common.SendMessage(client, line) {
			log.Warn("Download failed")
			return false
		}
	}

	client.Write([]byte("END\n"))
	client.Read(make([]byte, common.MIN_BUFFER_SIZE)) // wait for client to finish download
	return true
}
