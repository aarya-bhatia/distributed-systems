package filesystem

import (
	"bufio"
	"cs425/common"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"strconv"
	"strings"
)

func sendError(conn net.Conn, message string) {
	log.Warnf("(Client %s) Error: %s", conn.RemoteAddr(), message)
	common.SendMessage(conn, "ERROR\n"+message+"\n")
}

// Read all requests from client until an UPLOAD_FILE or DOWNLOAD_FILE request,
// which are then queued. Also disconnects on any errors.
func (server *Server) handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	isQueued := false

	// Close connection with client unless request was queued
	defer func() {
		if !isQueued {
			conn.Close()
		}
	}()

	for {
		buffer, err := reader.ReadString('\n') // Read from client until newline
		if err != nil {
			// log.Debugf("Client %s disconnected\n", conn.RemoteAddr())
			return
		}

		buffer = buffer[:len(buffer)-1] // Erase newline byte
		tokens := strings.Split(buffer, " ")
		verb := tokens[0]

		log.Debugf("Request from %s: %s", conn.RemoteAddr(), verb)

		switch {

		// To read a file from client
		case verb == "UPLOAD_FILE":
			if !server.handleUploadFile(conn, tokens) {
				return
			}

			isQueued = true
			return

		// To write a file to a client
		case verb == "DOWNLOAD_FILE":
			if !server.handleDownloadFile(conn, tokens) {
				return
			}

			isQueued = true
			return

		// To request file delete
		case verb == "DELETE_FILE":
			if !server.handleDeleteFile(conn, tokens) {
				return
			}

			isQueued = true
			return

		// To list file replicas
		case verb == "LIST_FILE":
			if !server.handleListFile(conn, tokens) {
				return
			}

		// To list files matching directory prefix
		case verb == "LIST_DIRECTORY":
			if !server.handleListDirectory(conn, tokens) {
				return
			}

		// To upload block at replica
		case verb == "UPLOAD":
			if !server.handleUploadBlock(conn, tokens) {
				return
			}

		// To download block at replica
		case verb == "DOWNLOAD":
			if !server.handleDownloadBlock(conn, tokens) {
				return
			}

		// To request node to download the block from another source
		case verb == "ADD_BLOCK":
			if !server.handleAddBlock(conn, tokens) {
				return
			}

		// Send the current leader node address
		case verb == "GET_LEADER":
			if !server.handleGetLeader(conn) {
				return
			}

		// To udpate block metadata
		case verb == "SETBLOCK":
			server.handleSetBlock(conn, tokens)

		// To udpate file metadata
		case verb == "SETFILE":
			server.handleSetFile(conn, tokens)

		// To delete file blocks and metadata
		case verb == "DELETE":
			server.handleDelete(conn, tokens)

		// client exit
		case verb == "BYE":
			return

		// exit server
		case verb == "KILL":
			os.Exit(1)

		default:
			log.Warn("Unknown verb")
			return
		}
	}
}

// Usage: UPLOAD_FILE <filename> <filesize>
func (server *Server) handleUploadFile(conn net.Conn, tokens []string) bool {
	if len(tokens) != 3 {
		sendError(conn, "Usage: UPLOAD_FILE <filename> <filesize>")
		return false
	}

	filename := tokens[1]
	filesize, err := strconv.Atoi(tokens[2])
	if err != nil || filesize <= 0 {
		sendError(conn, "File size must be a positive number")
		return false
	}

	// Add upload file request to queue and quit for now
	server.getQueue(filename).PushWrite(&Request{
		Action: UPLOAD_FILE,
		Client: conn,
		Name:   filename,
		Size:   filesize,
	})

	return true
}

// Usage: DELETE_FILE <filename>
func (server *Server) handleDeleteFile(conn net.Conn, tokens []string) bool {
	if len(tokens) != 2 {
		sendError(conn, "Usage: DELETE_FILE <filename>")
		return false
	}

	filename := tokens[1]

	// Add delete file request to queue and quit for now
	server.getQueue(filename).PushWrite(&Request{
		Action: DELETE_FILE,
		Client: conn,
		Name:   filename,
	})

	return true
}

// Usage: DOWNLOAD_FILE <filename>
func (server *Server) handleDownloadFile(conn net.Conn, tokens []string) bool {
	if len(tokens) != 2 {
		sendError(conn, "Usage: DOWNLOAD_FILE <filename>")
		return false
	}

	filename := tokens[1]

	// Add download file request to queue and quit for now
	server.getQueue(filename).PushRead(&Request{
		Action: DOWNLOAD_FILE,
		Client: conn,
		Name:   filename,
	})

	return true
}

// Usage: UPLOAD <blockName> <blockSize>
func (server *Server) handleUploadBlock(conn net.Conn, tokens []string) bool {
	if len(tokens) != 3 {
		sendError(conn, "Usage: UPLOAD <blockname> <blocksize>")
		return false
	}

	blockName := tokens[1]
	blockSize, err := strconv.Atoi(tokens[2])
	if err != nil || blockSize <= 0 {
		sendError(conn, "Block size must be a positive number")
		return false
	}
	if blockSize > common.BLOCK_SIZE {
		sendError(conn, fmt.Sprintf("Maximum block size is %d", common.BLOCK_SIZE))
		return false
	}

	if !uploadBlock(server.Directory, conn, blockName, blockSize) {
		common.SendMessage(conn, "ERROR")
		return false
	} else {
		if !common.SendMessage(conn, "OK") {
			return false
		}
	}

	return true
}

// Usage: DOWNLOAD <blockName>
func (server *Server) handleDownloadBlock(conn net.Conn, tokens []string) bool {
	if len(tokens) != 2 {
		sendError(conn, "Usage: DOWNLOAD <blockname>")
		return false
	}

	return downloadBlock(server.Directory, conn, tokens[1])
}

// Usage: ADD_BLOCK <name> <size> <source>
func (server *Server) handleAddBlock(conn net.Conn, tokens []string) bool {
	if len(tokens) != 4 {
		sendError(conn, "Usage: ADD_BLOCK <name> <size> <source>")
		return false
	}

	blockName := tokens[1]
	blockSize, err := strconv.Atoi(tokens[2])
	if err != nil || blockSize > common.BLOCK_SIZE {
		common.SendMessage(conn, "ERROR")
		return false
	}

	// check if block already exists
	if common.FileExists(server.Directory + "/" + blockName) {
		return common.SendMessage(conn, "OK")
	}

	source := tokens[3]

	if !replicateBlock(server.Directory, blockName, blockSize, source) {
		common.SendMessage(conn, "ERROR")

		return false
	}

	// update metadata (only matters if this is a metadata replica)
	server.Mutex.Lock()
	server.BlockToNodes[blockName] = common.AddUniqueElement(server.BlockToNodes[blockName], server.ID)
	server.NodesToBlocks[server.ID] = common.AddUniqueElement(server.NodesToBlocks[server.ID], blockName)
	server.Mutex.Unlock()

	return common.SendMessage(conn, "OK")
}

// Usage: GET_LEADER
func (server *Server) handleGetLeader(conn net.Conn) bool {
	return common.SendMessage(conn, server.GetLeaderNode()+"\n")
}

// Usage: SETFILE <filename> <version> <size> <numBlocks>
func (server *Server) handleSetFile(conn net.Conn, tokens []string) {
	if len(tokens) != 5 {
		common.SendMessage(conn, "SETFILE_ERROR")
		return
	}

	filename := tokens[1]
	version, _ := strconv.Atoi(tokens[2])
	size, _ := strconv.Atoi(tokens[3])
	numBlocks, _ := strconv.Atoi(tokens[4])

	server.Mutex.Lock()
	if found, ok := server.Files[filename]; ok && found.Version > version {
		server.Mutex.Unlock()
		common.SendMessage(conn, "SETFILE_ERROR")
		return
	}
	server.Files[filename] = File{Filename: filename, Version: version, FileSize: size, NumBlocks: numBlocks}
	server.Mutex.Unlock()
	common.SendMessage(conn, "SETFILE_OK")
}

// Usage: SETBLOCK <blockName> <replicas,...>
func (server *Server) handleSetBlock(conn net.Conn, tokens []string) {
	if len(tokens) != 3 {
		common.SendMessage(conn, "SETBLOCK_ERROR")
		return
	}

	blockName := tokens[1]
	replicas := strings.Split(tokens[2], ",")

	server.Mutex.Lock()
	for _, replica := range replicas {
		server.NodesToBlocks[replica] = common.AddUniqueElement(server.NodesToBlocks[replica], blockName)
		server.BlockToNodes[blockName] = common.AddUniqueElement(server.BlockToNodes[blockName], replica)
	}
	server.Mutex.Unlock()
	common.SendMessage(conn, "SETBLOCK_OK")
}

// Usage: DELETE <filename> <version> <numBlocks>
func (server *Server) handleDelete(conn net.Conn, tokens []string) {
	if len(tokens) != 4 {
		common.SendMessage(conn, "ERROR")
		return
	}

	filename := tokens[1]
	version, _ := strconv.Atoi(tokens[2])
	numBlocks, _ := strconv.Atoi(tokens[3])

	server.deleteFileLocal(File{Filename: filename, Version: version, NumBlocks: numBlocks})
	common.SendMessage(conn, "OK")
}

// Usage: LIST_FILE <filename>
func (server *Server) handleListFile(conn net.Conn, tokens []string) bool {
	if len(tokens) != 2 {
		return common.SendMessage(conn, "ERROR Malformed Request")
	}

	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	filename := tokens[1]
	file, ok := server.Files[filename]
	if !ok {
		return common.SendMessage(conn, "ERROR File not found")
	}

	if !common.SendMessage(conn, "LIST_START") {
		return false
	}

	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(filename, file.Version, i)
		replicas := server.BlockToNodes[blockName]
		reply := fmt.Sprintf("%s %s", blockName, strings.Join(replicas, ","))
		log.Println(reply, replicas)
		if !common.SendMessage(conn, reply) {
			return false
		}
	}

	return common.SendMessage(conn, "LIST_END")
}

// Usage: LIST_DIRECTORY <name>
func (server *Server) handleListDirectory(conn net.Conn, tokens []string) bool {
	if len(tokens) != 2 {
		return common.SendMessage(conn, "ERROR Malformed Request")
	}

	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	if !common.SendMessage(conn, "LIST_START") {
		return false
	}

	for file := range server.Files {
		if strings.Index(file, tokens[1]) == 0 {
			common.SendMessage(conn, file)
		}
	}

	return common.SendMessage(conn, "LIST_END")
}
