package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"strconv"
	"strings"
)

func handleUploadBlockRequest(client net.Conn, filename string, filesize int64, minAcks int) bool {
	return false
}

func handleDownloadFileRequest(server *Server, conn net.Conn, filename string) {
	file, ok := server.files[filename]

	if !ok {
		conn.Write([]byte("ERROR\nFile not found\n"))
		return
	}

	response := fmt.Sprintf("OK %s:%d:%d\n", filename, file.Version, file.FileSize)

	if !SendAll(conn, []byte(response), len(response)) {
		return
	}

	bytesSent := 0

	for i := 0; i < file.NumBlocks; i++ {
		blockName := GetBlockName(filename, file.Version, i)
		replica := server.info // TODO: select replicas
		if replica.ID == server.info.ID {
			block := server.storage[blockName]
			if !SendAll(conn, block.Data, block.Size) {
				return
			}

			bytesSent += block.Size
		}
	}

	log.Infof("Sent file %s (%d bytes) to client %s\n", filename, bytesSent, conn.RemoteAddr())
}

func handleUploadFileRequest(server *Server, client net.Conn, filename string, filesize int, minAcks int) bool {
	version := 1
	if oldFile, ok := server.files[filename]; ok {
		version = oldFile.Version + 1
	}

	client.Write([]byte("OK\n")) // Notify client to start uploading data

	buffer := make([]byte, BLOCK_SIZE)
	bytesRead := 0
	blockCount := 0

	for bytesRead < filesize {
		blockName := GetBlockName(filename, version, blockCount)

		n, err := client.Read(buffer)
		if err != nil {
			log.Println(err)
			return false
		}

		log.Debugf("Received block %d (%d bytes) from client %s", blockCount, n, client.RemoteAddr())

		if n < BLOCK_SIZE && bytesRead+n < filesize {
			log.Println("Insufficient bytes received")
			return false
		}

		// replica := GetReplicaNodes(blockName, 1)[0]
		replica := server.info // TODO

		if replica.ID == server.info.ID {
			data := make([]byte, n)
			copy(data, buffer[:n])
			server.storage[blockName] = &Block{Size: n, Data: data}
			log.Debugf("Added block %s to storage\n", blockName)
		} else {
			replicaAddr := fmt.Sprintf("%s:%d", replica.Hostname, replica.Port)
			replicaConn, err := net.Dial("tcp", replicaAddr)
			if err != nil {
				log.Println("Failed to establish connection", err)
				return false
			}

			request := fmt.Sprintf("UPLOAD:%s:%d:%d:%d\n", filename, version, blockCount, n)
			_, err = replicaConn.Write([]byte(request))
			if err != nil {
				log.Println("File upload failed", err)
				return false
			}

			_, err = replicaConn.Write([]byte(buffer[:n]))
			if err != nil {
				log.Println("File upload failed", err)
				return false
			}

			log.Debugf("Sent block %d to node %s", blockCount, replicaAddr)
			replicaConn.Close()
		}

		server.nodesToBlocks[replica.ID] = append(server.nodesToBlocks[replica.ID], blockName)
		server.blockToNodes[blockName] = append(server.blockToNodes[blockName], replica.ID)

		bytesRead += n
		blockCount += 1
	}

	// TODO: Receive acks
	// for {
	// 	select {
	// 	case e := <-ackChannel:
	// 	case e := <-failureDetectorChannel:
	// 	}
	// }

	server.files[filename] = &File{Filename: filename, Version: 1, FileSize: filesize, NumBlocks: blockCount}
	return true
}

func StartServer(server *Server) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.info.Port))

	if err != nil {
		log.Fatal("Error starting server", err)
	}
	defer listener.Close()

	log.Infof("TCP Server is listening on port %d...\n", server.info.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Warnf("Error accepting connection: %s\n", err)
			continue
		}

		log.Debugf("Accepted connection from %s\n", conn.RemoteAddr())
		go handleTCPConnection(server, conn)
	}
}

func handleTCPConnection(server *Server, conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, MIN_BUFFER_SIZE)
	n, err := conn.Read(buffer)
	if err != nil {
		log.Warnf("Client %s disconnected\n", conn.RemoteAddr())
		return
	}

	request := string(buffer[:n])
	log.Debugf("Received request from %s: %s\n", conn.RemoteAddr(), request)

	lines := strings.Split(request, "\n")
	tokens := strings.Split(lines[0], " ")
	verb := tokens[0]

	if verb == "UPLOAD_FILE" {
		filename := tokens[1]
		filesize, err := strconv.Atoi(tokens[2])
		if err != nil {
			log.Warn(err)
			return
		}
		if handleUploadFileRequest(server, conn, filename, filesize, 1) {
			conn.Write([]byte("OK\n"))
		} else {
			conn.Write([]byte("ERROR\n"))
		}
	} else if verb == "DOWNLOAD_FILE" {
		filename := tokens[1]
		handleDownloadFileRequest(server, conn, filename)
	} else {
		log.Warn("Unknown verb: ", verb)
	}
}
