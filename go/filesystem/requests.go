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

func (server *Server) handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)

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
			if len(tokens) != 3 {
				sendError(conn, "Usage: UPLOAD_FILE <filename> <filesize>")
				break
			}

			filename := tokens[1]
			filesize, err := strconv.Atoi(tokens[2])
			if err != nil || filesize <= 0 {
				sendError(conn, "File size must be a positive number")
				break
			}

			// Add upload file request to queue and quit for now
			server.getQueue(filename).PushWrite(&Request{
				Action: UPLOAD_FILE,
				Client: conn,
				Name:   filename,
				Size:   filesize,
			})

			return

		// To write a file to a client
		case verb == "DOWNLOAD_FILE":
			if len(tokens) != 2 {
				sendError(conn, "Usage: DOWNLOAD_FILE <filename>")
				break
			}

			filename := tokens[1]
			Log.Debugf("Received download request for file %s\n", filename)

			// Add download file request to queue and quit for now
			server.getQueue(filename).PushRead(&Request{
				Action: DOWNLOAD_FILE,
				Client: conn,
				Name:   filename,
			})

			return

		// To upload block at replica
		case verb == "UPLOAD":
			if len(tokens) != 3 {
				sendError(conn, "Usage: UPLOAD_BLOCK <blockname> <blocksize>")
				break
			}

			blockName := tokens[1]
			blockSize, err := strconv.Atoi(tokens[2])
			if err != nil || blockSize <= 0 {
				sendError(conn, "Block size must be a positive number")
				break
			}
			if blockSize > common.BLOCK_SIZE {
				sendError(conn, fmt.Sprintf("Maximum block size is %d", common.BLOCK_SIZE))
				break
			}

			uploadBlock(server.Directory, conn, blockName, blockSize)

		// To download block at replica
		case verb == "DOWNLOAD":
			if len(tokens) != 2 {
				sendError(conn, "Usage: DOWNLOAD <blockname>")
				break
			}

			downloadBlock(server.Directory, conn, tokens[1])

		// To request node to download the block from another source
		case verb == "ADD_BLOCK":
			if len(tokens) != 4 {
				sendError(conn, "Usage: ADD_BLOCK <name> <size> <source>")
				break
			}

			blockName := tokens[1]
			blockSize, err := strconv.Atoi(tokens[2])
			if err != nil || blockSize > common.BLOCK_SIZE {
				sendError(conn, fmt.Sprintf("Block size must be positive integer less than %d", common.BLOCK_SIZE))
				break
			}

			source := tokens[3]
			server.replicateBlock(conn, blockName, blockSize, source)

		// Send the current leader node address
		case verb == "GET_LEADER":
			_, err = conn.Write([]byte(server.GetLeaderNode() + "\n"))
			if err != nil {
				conn.Close()
				return
			}

		case verb == "INFO":
			server.Mutex.Lock()
			response := fmt.Sprintf("ID: %s, %d files, %d blocks, %d nodes\n",
				server.ID,
				len(server.Files),
				len(server.BlockToNodes),
				len(server.Nodes))
			server.Mutex.Unlock()

			if common.SendAll(conn, []byte(response), len(response)) < 0 {
				conn.Close()
				return
			}

		case verb == "BYE":
			conn.Close()
			return

		default:
			sendError(conn, "Unknown verb")
		}
	}
}
