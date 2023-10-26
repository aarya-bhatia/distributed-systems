package filesystem

import (
	"bufio"
	"cs425/common"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func sendError(conn net.Conn, message string) {
	Log.Warnf("(Client %s) Error: %s", conn.RemoteAddr(), message)
	conn.Write([]byte("ERROR\n" + message + "\n"))
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
			Log.Warnf("Client %s disconnected\n", conn.RemoteAddr())
			return
		}

		Log.Debugf("Received request from %s: %s", conn.RemoteAddr(), string(buffer))
		buffer = buffer[:len(buffer)-1] // Erase newline byte
		tokens := strings.Split(buffer, " ")
		verb := tokens[0]

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

		// To delete file
		case verb == "DELETE_FILE":
			if !server.handleDeleteFile(conn, tokens) {
				return
			}

			isQueued = true
			return

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

		// To delete block at replica
		case verb == "DELETE":
			if !server.handleDeleteBlock(conn, tokens) {
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

		// client exit
		case verb == "BYE":
			return

		default:
			Log.Warn("Unknown verb")
			return
		}
	}
}

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

func (server *Server) handleUploadBlock(conn net.Conn, tokens []string) bool {
	if len(tokens) != 3 {
		sendError(conn, "Usage: UPLOAD_BLOCK <blockname> <blocksize>")
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

func (server *Server) handleDownloadBlock(conn net.Conn, tokens []string) bool {
	if len(tokens) != 2 {
		sendError(conn, "Usage: DOWNLOAD <blockname>")
		return false
	}

	return downloadBlock(server.Directory, conn, tokens[1])
}

func (server *Server) handleDeleteBlock(conn net.Conn, tokens []string) bool {
	if len(tokens) != 2 {
		sendError(conn, "Usage: DELETE <blockname>")
		return false
	}

	return deleteBlock(server.Directory, conn, tokens[1])
}

func (server *Server) handleAddBlock(conn net.Conn, tokens []string) bool {
	if len(tokens) != 4 {
		sendError(conn, "Usage: ADD_BLOCK <name> <size> <source>")
		return false
	}

	blockName := tokens[1]
	blockSize, err := strconv.Atoi(tokens[2])
	if err != nil || blockSize > common.BLOCK_SIZE {
		sendError(conn, fmt.Sprintf("Block size must be positive integer less than %d", common.BLOCK_SIZE))
		return false
	}

	source := tokens[3]

	if replicateBlock(server.Directory, blockName, blockSize, source) {
		if !common.SendMessage(conn, "OK") {
			return false
		}
	} else {
		common.SendMessage(conn, "ERROR")
		return false
	}

	return true
}

func (server *Server) handleGetLeader(conn net.Conn) bool {
	_, err := conn.Write([]byte(server.GetLeaderNode() + "\n"))
	if err != nil {
		conn.Close()
		return false
	}

	return true
}
