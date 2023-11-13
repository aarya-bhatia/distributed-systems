package server

import (
	"bufio"
	"cs425/common"
	"fmt"
	"net"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

type FileServer struct {
	Hostname  string
	TCPPort   int
	Directory string
}

func (s *FileServer) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.Hostname, s.TCPPort))
	if err != nil {
		log.Fatal(err)
	}

	log.Info("SDFS file server is listening at", listener.Addr())

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *FileServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			// log.Println(err)
			return
		}

		line = line[:len(line)-1]
		tokens := strings.Split(line, " ")

		switch tokens[0] {

		case "UPLOAD":
			if len(tokens) < 3 {
				return
			}
			name := tokens[1]
			size, err := strconv.Atoi(tokens[2])
			if err != nil {
				return
			}
			if !s.upload(conn, name, size) {
				common.SendMessage(conn, "ERROR")
				return
			}
			if !common.SendMessage(conn, "OK") {
				return
			}

		case "DOWNLOAD":
			if len(tokens) < 2 {
				return
			}
			if !s.download(conn, tokens[1]) {
				return
			}

		default:
			log.Warn("Unknown request")
			return
		}
	}
}

// Read a file block from disk and send it to client
func (s *FileServer) download(client net.Conn, file string) bool {
	log.Debugf("Sending file %s to client %s", file, client.RemoteAddr())
	file = common.DecodeFilename(file)
	if buffer := common.ReadFile(s.Directory, file); buffer != nil {
		return common.SendAll(client, buffer, len(buffer)) > 0
	}
	return false
}

// Receive a file block from client and write it to disk
func (s *FileServer) upload(client net.Conn, name string, size int) bool {
	buffer := make([]byte, common.BLOCK_SIZE)
	bufferSize := 0

	if !common.SendMessage(client, "OK") {
		return false
	}

	for bufferSize < size {
		numRead, err := client.Read(buffer[bufferSize:])
		if err != nil {
			log.Warn(err)
			return false
		}
		if numRead == 0 {
			break
		}
		bufferSize += numRead
	}

	if bufferSize < size {
		log.Warnf("Insufficient bytes read (%d of %d)\n", bufferSize, size)
		return false
	}

	log.Debugf("Received block %s (%d bytes) from client %s", name, size, client.RemoteAddr())

	name = common.EncodeFilename(name)
	return common.WriteFile(s.Directory, name, buffer, size)
}

// Download a block from source node to replicate it at current node
// TODO: replica all blocks
func replicateBlock(directory string, blockName string, blockSize int, source int) bool {
	node := common.GetNodeByID(source)
	addr := common.GetAddress(node.Hostname, node.TCPPort)

	log.Debugf("To replicate block %s from host %s\n", blockName, addr)

	repliaConn, err := net.Dial("tcp", addr)
	if err != nil {
		return false
	}
	defer repliaConn.Close()

	if !common.SendMessage(repliaConn, fmt.Sprintf("DOWNLOAD %s\n", blockName)) {
		return false
	}

	buffer := make([]byte, common.BLOCK_SIZE)
	size := 0
	for size < blockSize {
		n, err := repliaConn.Read(buffer[size:])
		if err != nil {
			return false
		}
		if n == 0 {
			break
		}
		size += n
	}

	return common.WriteFile(directory, blockName, buffer, size)
}
