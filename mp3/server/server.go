package main

import (
	"cs425/mp3/common"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Block struct {
	Size int
	Data []byte
}

type File struct {
	Filename  string
	Version   int
	FileSize  int
	NumBlocks int
}

type Request struct {
	RequestType int
	Action      int
	Filename    string
	ClientID    int
}

type NodeInfo struct {
	ID       int
	Hostname string
	Port     int
	State    int
}

type Server struct {
	files         map[string]*File  // Files stored by system
	storage       map[string]*Block // In memory data storage
	nodesToBlocks map[int][]string  // Maps node to the blocks they are storing
	blockToNodes  map[string][]int  // Maps block to list of nodes that store the block
	info          *NodeInfo         // Info about current server
	// var ackChannel chan string
	// var failureDetectorChannel chan string
	// var queue []*Request                 // A queue of requests
}

func NewServer(info *NodeInfo) *Server {
	server := new(Server)
	server.info = info
	server.files = make(map[string]*File)
	server.storage = make(map[string]*Block)
	server.nodesToBlocks = make(map[int][]string)
	server.blockToNodes = make(map[string][]int)
	log.Info("Server", *server, "Info", *info)
	return server
}

// Returns slice of alive nodes
func GetAliveNodes(nodes []*NodeInfo) []*NodeInfo {
	res := []*NodeInfo{}
	for i, node := range nodes {
		if node.State == STATE_ALIVE {
			res = append(res, nodes[i])
		}
	}
	return res
}

// The first R nodes with the lowest ID are selected as metadata replicas.
func GetMetadataReplicaNodes(count int) []*NodeInfo {
	aliveNodes := GetAliveNodes(nodes)
	if count < len(aliveNodes) {
		return aliveNodes[:count]
	}
	return aliveNodes
}

// The nearest node to the hash of the filename is selected as primary replica.
// The next R-1 successors are selected as backup replicas.
func GetReplicaNodes(filename string, count int) []*NodeInfo {
	aliveNodes := GetAliveNodes(nodes)
	if len(aliveNodes) < count {
		return aliveNodes
	}

	hash := common.GetHash(filename, len(nodes))
	res := []*NodeInfo{}

	for i := 0; i < len(nodes); i++ {
		j := (hash + i) % len(nodes)
		if nodes[j].State == STATE_ALIVE {
			res = append(res, nodes[j])
			if len(res) >= count {
				break
			}
		}
	}

	return res
}

func UploadBlock(client net.Conn, filename string, filesize int64, minAcks int) bool {
	return false
}

// Send file to client
func DownloadFile(server *Server, conn net.Conn, filename string) {
	file, ok := server.files[filename]

	if !ok {
		conn.Write([]byte("ERROR\nFile not found\n"))
		return
	}

	response := fmt.Sprintf("OK %s:%d:%d\n", filename, file.Version, file.FileSize)

	if common.SendAll(conn, []byte(response), len(response)) < 0 {
		return
	}

	bytesSent := 0

	log.Debugf("To send %d blocks\n", file.NumBlocks)

	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(filename, file.Version, i)
		replicas := server.blockToNodes[blockName]

		if len(replicas) == 0 {
			log.Warnf("No replicas found for block %d\n", i)
			return
		}

		log.Debugf("Replicas for block %d: %v\n", i, replicas)

		replicaID := replicas[rand.Intn(len(replicas))]
		replica := nodes[replicaID-1]

		if replica.ID == server.info.ID {
			block := server.storage[blockName]
			if common.SendAll(conn, block.Data, block.Size) < 0 {
				log.Warn("Failed to send block")
				return
			}

			bytesSent += block.Size
		} else {
			log.Debugf("Replica %d is unavailable\n", replica.ID)
		}
	}

	log.Infof("Sent file %s (%d bytes) to client %s\n", filename, bytesSent, conn.RemoteAddr())
}

func processUploadBlock(server *Server, blockName string, buffer []byte, blockSize int) {
	blockData := make([]byte, blockSize)
	copy(blockData, buffer[:blockSize])

	// replica := GetReplicaNodes(blockName, 1)[0]
	replica := server.info

	if replica.ID == server.info.ID {
		block := &Block{Size: blockSize, Data: blockData}
		server.storage[blockName] = block
		log.Debugf("Added block %s to storage\n", blockName)
	}

	// } else {
	// 	replicaAddr := fmt.Sprintf("%s:%d", replica.Hostname, replica.Port)
	// 	replicaConn, err := net.Dial("tcp", replicaAddr)
	// 	if err != nil {
	// 		log.Println("Failed to establish connection", err)
	// 		return false
	// 	}
	//
	// 	request := fmt.Sprintf("UPLOAD:%s:%d:%d:%d\n", filename, version, i, n)
	// 	_, err = replicaConn.Write([]byte(request))
	// 	if err != nil {
	// 		log.Println("File upload failed", err)
	// 		return false
	// 	}
	//
	// 	_, err = replicaConn.Write([]byte(buffer[:n]))
	// 	if err != nil {
	// 		log.Println("File upload failed", err)
	// 		return false
	// 	}
	//
	// 	log.Debugf("Sent block %d to node %s", i, replicaAddr)
	// 	replicaConn.Close()
	// }

	server.nodesToBlocks[replica.ID] = append(server.nodesToBlocks[replica.ID], blockName)
	server.blockToNodes[blockName] = append(server.blockToNodes[blockName], replica.ID)
}

func UploadFile(server *Server, client net.Conn, filename string, filesize int, minAcks int) bool {
	version := 1
	if oldFile, ok := server.files[filename]; ok {
		version = oldFile.Version + 1
	}

	client.Write([]byte("OK\n")) // Notify client to start uploading data

	numBlocks := common.GetNumFileBlocks(int64(filesize))
	log.Debugf("To upload %d blocks\n", numBlocks)

	buffer := make([]byte, common.BLOCK_SIZE)
	bufferSize := 0
	bytesRead := 0
	blockCount := 0

	for bytesRead < filesize {
		numRead, err := client.Read(buffer[bufferSize:])
		if err != nil {
			log.Println(err)
			return false
		}

		if numRead == 0 {
			break
		}

		bufferSize += numRead

		if bufferSize == common.BLOCK_SIZE {
			log.Debugf("Received block %d (%d bytes) from client %s", blockCount, bufferSize, client.RemoteAddr())
			blockName := common.GetBlockName(filename, version, blockCount)
			processUploadBlock(server, blockName, buffer, bufferSize)
			bufferSize = 0
			blockCount += 1
		}

		bytesRead += numRead
	}

	if bufferSize > 0 {
		log.Debugf("Received block %d (%d bytes) from client %s", blockCount, bufferSize, client.RemoteAddr())
		blockName := common.GetBlockName(filename, version, blockCount)
		processUploadBlock(server, blockName, buffer, bufferSize)
	}

	if bytesRead < filesize {
		log.Warnf("Insufficient bytes read (%d of %d)\n", bytesRead, filesize)
		return false
	}

	// TODO: Receive acks
	// for {
	// 	select {
	// 	case e := <-ackChannel:
	// 	case e := <-failureDetectorChannel:
	// 	}
	// }

	server.files[filename] = &File{Filename: filename, Version: version, FileSize: filesize, NumBlocks: numBlocks}
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
		go clientProtocol(server, conn)
	}
}

func clientProtocol(server *Server, conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, common.MIN_BUFFER_SIZE)
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
		if UploadFile(server, conn, filename, filesize, 1) {
			log.Debug("Upload OK")
			conn.Write([]byte("OK\n"))
		} else {
			log.Debug("Upload ERROR")
			conn.Write([]byte("ERROR\n"))
		}
	} else if verb == "DOWNLOAD_FILE" {
		filename := tokens[1]
		DownloadFile(server, conn, filename)
	} else {
		log.Warn("Unknown verb: ", verb)
	}
}
