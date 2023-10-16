package filesystem

import (
	"container/heap"
	"cs425/common"
	"cs425/priqueue"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	REQUEST_READ  = 0
	REQUEST_WRITE = 1

	STATE_ALIVE  = 0
	STATE_FAILED = 1
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

type Server struct {
	Info          common.Node
	Files         map[string]*File    // Files stored by system
	Storage       map[string]*Block   // In memory data storage
	NodesToBlocks map[int][]string    // Maps node to the blocks they are storing
	BlockToNodes  map[string][]int    // Maps block to list of nodes that store the block
	Nodes         map[int]common.Node // Set of alive nodes
	// var ackChannel chan string
	// var failureDetectorChannel chan string
	// var queue []*Request                 // A queue of requests
	InputChannel chan string
}

var Log *common.Logger = common.Log

func (s *Server) HandleNodeJoin(node *common.Node) {
	Log.Debug("node joined: ", *node)
	s.Nodes[node.ID] = *node
	s.NodesToBlocks[node.ID] = []string{}
	s.Rebalance()
}

func (s *Server) HandleNodeLeave(node *common.Node) {
	Log.Debug("node left: ", *node)
	delete(s.Nodes, node.ID)
	for _, block := range s.NodesToBlocks[node.ID] {
		for i := 0; i < len(s.BlockToNodes[block]); i++ {
			nodes := s.BlockToNodes[block]
			if nodes[i] == node.ID {
				s.BlockToNodes[block] = common.RemoveIndex(nodes, i)
				break
			}
		}
	}
	delete(s.NodesToBlocks, node.ID)
	s.Rebalance()
}

func NewServer(info common.Node) *Server {
	server := new(Server)
	server.Info = info
	server.Files = make(map[string]*File)
	server.Storage = make(map[string]*Block)
	server.NodesToBlocks = make(map[int][]string)
	server.BlockToNodes = make(map[string][]int)
	server.InputChannel = make(chan string)
	server.Nodes = make(map[int]common.Node)
	server.Nodes[info.ID] = info

	return server
}

// Node with smallest ID
func (s *Server) GetLeaderNode(count int) int {
	pq := make(priqueue.PriorityQueue, 0)
	heap.Init(&pq)

	for ID := range s.Nodes {
		heap.Push(&pq, &priqueue.Item{Key: ID})
	}

	item := heap.Pop(&pq).(*priqueue.Item)
	return item.Key
}

// The first R nodes with the lowest ID are selected as metadata replicas.
func (s *Server) GetMetadataReplicaNodes(count int) []int {
	pq := make(priqueue.PriorityQueue, 0)
	heap.Init(&pq)

	for ID := range s.Nodes {
		heap.Push(&pq, &priqueue.Item{Key: ID})
	}

	res := []int{}
	for r := 0; r < count && pq.Len() > 0; r++ {
		item := heap.Pop(&pq).(*priqueue.Item)
		res = append(res, item.Key)
	}

	return res
}

// Get the `count` nearest nodes to the file hash
func (s *Server) GetReplicaNodes(filename string, count int) []int {
	fileHash := common.GetHash(filename, len(common.Cluster))
	pq := make(priqueue.PriorityQueue, 0)
	heap.Init(&pq)

	for ID := range s.Nodes {
		distance := ID - fileHash
		if distance < 0 {
			distance = -distance
		}

		heap.Push(&pq, &priqueue.Item{Key: distance, Value: ID})
	}

	res := []int{}
	for r := 0; r < count && pq.Len() > 0; r++ {
		item := heap.Pop(&pq).(*priqueue.Item)
		res = append(res, item.Value)
	}

	return res
}

func (server *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Info.TCPPort))

	if err != nil {
		Log.Fatal("Error starting server", err)
	}
	defer listener.Close()

	Log.Infof("TCP Server is listening on port %d...\n", server.Info.TCPPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			Log.Warnf("Error accepting connection: %s\n", err)
			continue
		}

		Log.Debugf("Accepted connection from %s\n", conn.RemoteAddr())
		go server.clientProtocol(conn)
	}
}

// Print file system metadata information to stdout
func PrintFileMetadata(server *Server) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Filename", "Version", "Block", "Nodes"})

	rows := []table.Row{}

	for blockName, nodeIds := range server.BlockToNodes {
		tokens := strings.Split(blockName, ":")
		filename, version, block := tokens[0], tokens[1], tokens[2]

		for _, id := range nodeIds {
			rows = append(rows, table.Row{
				filename,
				version,
				block,
				id,
			})
		}
	}

	t.AppendRows(rows)
	t.AppendSeparator()
	t.SetStyle(table.StyleLight)
	t.Render()
}

// To handle replicas after a node fails or rejoins
func (s *Server) Rebalance() {
	// m := map[int]bool{}
	//
	//	for _, node := range nodes {
	//		m[node.ID] = node.State == STATE_ALIVE
	//	}
	//
	// // Get affected replicas
	// // Add tasks to replicate the affected blocks to queue
	//
	//	for _, file := range files {
	//		expectedReplicas := GetReplicaNodes(file.Filename, REPLICA_FACTOR)
	//		for i := 0; i < file.NumBlocks; i++ {
	//			blockInfo := BlockInfo{Filename: file.Filename, Version: file.Version, ID: i}
	//			_, ok := blockToNodes[blockInfo]
	//			if !ok {
	//				for _, replica := range expectedReplicas {
	//					AddTask(UPDATE_BLOCK, replica, blockInfo)
	//				}
	//			} else {
	//				// TODO: AddTask for Intersection nodes
	//			}
	//		}
	//	}
}

func (server *Server) UploadBlock(client net.Conn, filename string, version int, blockNum int, blockSize int) bool {
	if oldFile, ok := server.Files[filename]; ok {
		if oldFile.Version > version {
			Log.Warn("Has newer version of block")
			return false
		}
	}

	// Notify client to start uploading data
	if common.SendAll(client, []byte("OK\n"), 3) < 0 {
		return false
	}

	buffer := make([]byte, common.BLOCK_SIZE)
	bufferSize := 0

	for bufferSize < blockSize {
		numRead, err := client.Read(buffer[bufferSize:])
		if err != nil {
			Log.Warn(err)
			return false
		}

		if numRead == 0 {
			break
		}

		bufferSize += numRead
	}

	if bufferSize < blockSize {
		Log.Warnf("Insufficient bytes read (%d of %d)\n", bufferSize, blockSize)
		return false
	}

	// Notify client of successful upload
	if common.SendAll(client, []byte("OK\n"), 3) < 0 {
		return false
	}

	Log.Debugf("Received block %d (%d bytes) from client %s", blockNum, blockSize, client.RemoteAddr())

	blockName := common.GetBlockName(filename, version, blockNum)
	block := &Block{Size: blockSize, Data: buffer[:blockSize]}
	server.Storage[blockName] = block
	server.NodesToBlocks[server.Info.ID] = append(server.NodesToBlocks[server.Info.ID], blockName)
	server.BlockToNodes[blockName] = append(server.BlockToNodes[blockName], server.Info.ID)

	Log.Debugf("Added block %s to storage\n", blockName)

	return true
}

func DownloadBlock(server *Server, conn net.Conn, filename string) {
}

// Send file to client
func (server *Server) DownloadFile(conn net.Conn, filename string) {
	file, ok := server.Files[filename]

	if !ok {
		conn.Write([]byte("ERROR\nFile not found\n"))
		return
	}

	response := fmt.Sprintf("OK %s:%d:%d\n", filename, file.Version, file.FileSize)

	if common.SendAll(conn, []byte(response), len(response)) < 0 {
		return
	}

	bytesSent := 0

	Log.Debugf("To send %d blocks\n", file.NumBlocks)

	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(filename, file.Version, i)
		replicas := server.BlockToNodes[blockName]

		if len(replicas) == 0 {
			Log.Warnf("No replicas found for block %d\n", i)
			return
		}

		replicaID := replicas[rand.Intn(len(replicas))]
		replica := common.GetNode(replicaID)

		if replica.ID == server.Info.ID {
			block := server.Storage[blockName]
			if common.SendAll(conn, block.Data, block.Size) < 0 {
				Log.Warn("Failed to send block")
				return
			}

			bytesSent += block.Size
		} else {
			Log.Debugf("Replica %d is unavailable\n", replica.ID)
		}
	}

	Log.Infof("Sent file %s (%d bytes) to client %s\n", filename, bytesSent, conn.RemoteAddr())
}

func (server *Server) processUploadBlock(blockName string, buffer []byte, blockSize int) bool {
	blockData := make([]byte, blockSize)
	copy(blockData, buffer[:blockSize])

	replica := server.GetReplicaNodes(blockName, 1)[0]

	if replica == server.Info.ID {
		block := &Block{Size: blockSize, Data: blockData}
		server.Storage[blockName] = block
		Log.Debugf("Added block %s to storage\n", blockName)
	} else {
		replicaInfo := common.GetNode(replica)
		replicaConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", replicaInfo.Hostname, replicaInfo.TCPPort))
		if err != nil {
			Log.Debug("Failed to establish connection", err)
			return false
		}

		request := fmt.Sprintf("UPLOAD %s %d\n", blockName, blockSize)
		Log.Debug("Sending upload block request to node ", replica)
		if common.SendAll(replicaConn, []byte(request), len(request)) < 0 {
			return false
		}

		Log.Debug("Waiting for confirmation from node ", replica)
		if !common.GetOKMessage(replicaConn) {
			return false
		}

		Log.Debug("Sending block to node ", replica)
		if common.SendAll(replicaConn, buffer[:blockSize], blockSize) < 0 {
			return false
		}

		Log.Debugf("Sent block %s to node %v", blockName, replicaInfo)

		Log.Debug("Waiting for confirmation from node ", replica)
		if !common.GetOKMessage(replicaConn) {
			return false
		}

		replicaConn.Close()
	}

	server.NodesToBlocks[replica] = append(server.NodesToBlocks[replica], blockName)
	server.BlockToNodes[blockName] = append(server.BlockToNodes[blockName], replica)
	return true
}

func (server *Server) UploadFile(client net.Conn, filename string, filesize int, minAcks int) bool {
	version := 1
	if oldFile, ok := server.Files[filename]; ok {
		version = oldFile.Version + 1
	}

	client.Write([]byte("OK\n")) // Notify client to start uploading data

	numBlocks := common.GetNumFileBlocks(int64(filesize))
	Log.Debugf("To upload %d blocks\n", numBlocks)

	buffer := make([]byte, common.BLOCK_SIZE)
	bufferSize := 0
	bytesRead := 0
	blockCount := 0

	for bytesRead < filesize {
		numRead, err := client.Read(buffer[bufferSize:])
		if err != nil {
			Log.Debug(err)
			return false
		}

		if numRead == 0 {
			break
		}

		bufferSize += numRead

		if bufferSize == common.BLOCK_SIZE {
			Log.Debugf("Received block %d (%d bytes) from client %s", blockCount, bufferSize, client.RemoteAddr())
			blockName := common.GetBlockName(filename, version, blockCount)
			if !server.processUploadBlock(blockName, buffer, bufferSize) {
				Log.Warn("Failed to upload block")
				return false
			}
			bufferSize = 0
			blockCount += 1
		}

		bytesRead += numRead
	}

	if bufferSize > 0 {
		Log.Debugf("Received block %d (%d bytes) from client %s", blockCount, bufferSize, client.RemoteAddr())
		blockName := common.GetBlockName(filename, version, blockCount)
		if !server.processUploadBlock(blockName, buffer, bufferSize) {
			Log.Warn("Failed to upload block")
			return false
		}
	}

	if bytesRead < filesize {
		Log.Warnf("Insufficient bytes read (%d of %d)\n", bytesRead, filesize)
		return false
	}

	// TODO: Receive acks
	// for {
	// 	select {
	// 	case e := <-ackChannel:
	// 	case e := <-failureDetectorChannel:
	// 	}
	// }

	server.Files[filename] = &File{Filename: filename, Version: version, FileSize: filesize, NumBlocks: numBlocks}
	client.Write([]byte("OK\n"))
	return true
}

func (server *Server) clientProtocol(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, common.MIN_BUFFER_SIZE)
	n, err := conn.Read(buffer)
	if err != nil {
		Log.Warnf("Client %s disconnected\n", conn.RemoteAddr())
		return
	}

	request := string(buffer[:n])
	Log.Debugf("Received request from %s: %s\n", conn.RemoteAddr(), request)

	lines := strings.Split(request, "\n")
	tokens := strings.Split(lines[0], " ")
	verb := tokens[0]

	if verb == "UPLOAD_FILE" {
		filename := tokens[1]
		filesize, err := strconv.Atoi(tokens[2])
		if err != nil {
			Log.Warn(err)
			return
		}
		if !server.UploadFile(conn, filename, filesize, 1) {
			conn.Write([]byte("ERROR\n"))
		}
	} else if verb == "DOWNLOAD_FILE" {
		filename := tokens[1]
		server.DownloadFile(conn, filename)
	} else if verb == "UPLOAD" { // To upload block at replica
		blockName := tokens[1]
		blockSize, err := strconv.Atoi(tokens[2])
		if err != nil {
			Log.Warn(err)
			return
		}
		tokens = strings.Split(blockName, ":")
		filename := tokens[0]
		version, err := strconv.Atoi(tokens[1])
		if err != nil {
			Log.Warn(err)
			return
		}
		blockNum, err := strconv.Atoi(tokens[2])
		if err != nil {
			Log.Warn(err)
			return
		}
		if !server.UploadBlock(conn, filename, version, blockNum, blockSize) {
			conn.Write([]byte("ERROR\n"))
			return
		}
	} else if verb == "DOWNLOAD" { // To download blockt at replica
	} else {
		Log.Warn("Unknown verb: ", verb)
	}
}

func (server *Server) HandleCommand(command string) {
	if command == "help" {
		fmt.Println("ls: Display metadata table")
		fmt.Println("info: Display server info")
		fmt.Println("files: Display list of files")
		fmt.Println("blocks: Display list of blocks")
	} else if command == "ls" {
		PrintFileMetadata(server)
	} else if command == "files" {
		for name, block := range server.Storage {
			tokens := strings.Split(name, ":")
			fmt.Printf("File %s, version %s, block %s, size %d\n", tokens[0], tokens[1], tokens[2], block.Size)
		}
	} else if command == "blocks" {
		for name, arr := range server.BlockToNodes {
			fmt.Printf("Block %s: ", name)
			for _, node := range arr {
				fmt.Printf("%d ", node)
			}
			fmt.Print("\n")
		}
	} else if command == "info" {
		fmt.Printf("ID: %d, Hostname: %s, Port: %d\n", server.Info.ID, server.Info.Hostname, server.Info.TCPPort)
		fmt.Printf("Num files: %d, Num blocks: %d, Num nodes: %d\n", len(server.Files), len(server.BlockToNodes), len(server.NodesToBlocks))
	}
}
