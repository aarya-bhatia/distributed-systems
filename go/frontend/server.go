package frontend

import (
	"bufio"
	"cs425/common"
	"cs425/filesystem"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type FrontendServer struct {
	Port          int
	BackendServer *filesystem.Server
}

type UploadInfo struct {
	server    net.Conn
	blockSize int
	blockData []byte
	blockName string
	replicas  []string
}

const MalformedRequestError = "ERROR Malformed Request"
const UnknownRequestError = "ERROR Unknown Request"

func NewServer(info common.Node, backendServer *filesystem.Server) *FrontendServer {
	server := new(FrontendServer)
	server.Port = info.FrontendPort
	server.BackendServer = backendServer
	return server
}

func (server *FrontendServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		buffer, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		if buffer == "\n" || len(buffer) == 0 {
			continue
		}

		tokens := strings.Split(buffer[:len(buffer)-1], " ")
		verb := tokens[0]

		if verb == "put" {
			if len(tokens) != 3 {
				common.SendMessage(conn, MalformedRequestError)
				return
			}

			if !server.uploadFile(tokens[1], tokens[2]) {
				log.Warn("Upload failed!")
				common.SendMessage(conn, "UPLOAD_ERROR")
			} else {
				log.Debug("Upload successful!")
				common.SendMessage(conn, "UPLOAD_OK")
			}

		} else if verb == "get" {
			if len(tokens) != 3 {
				common.SendMessage(conn, MalformedRequestError)
				return
			}

			if !server.downloadFile(tokens[2], tokens[1]) {
				log.Debug("Download failed!")
				common.SendMessage(conn, "DOWNLOAD_ERROR")
			} else {
				log.Debug("Download success!")
				common.SendMessage(conn, "DOWNLOAD_OK")
			}

		} else if verb == "delete" {
			if len(tokens) != 2 {
				common.SendMessage(conn, MalformedRequestError)
				return
			}

			if !server.deleteFile(tokens[1]) {
				log.Warn("Delete failed!")
				common.SendMessage(conn, "DELETE_ERROR")
			} else {
				log.Info("Delete successful!")
				common.SendMessage(conn, "DELETE_OK")
			}

		} else {
			common.SendMessage(conn, UnknownRequestError)
			return
		}
	}

}

func (server *FrontendServer) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Port))
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Frontend TCP server is listening on port %d...\n", server.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		// log.Debugf("Accepted connection from %s\n", conn.RemoteAddr())
		go server.handleConnection(conn)
	}
}

// First attempt to connect to SDFS node running locally and get the current
// leader. Then, attempt to connect to the leader node.
func (server *FrontendServer) getLeaderConnection() net.Conn {
	leader := server.BackendServer.GetLeaderNode()
	conn, err := net.Dial("tcp", leader)
	if err != nil {
		log.Warn("Leader is offline:", leader)
		return nil
	}

	log.Info("Connected to leader:", leader)
	return conn
}

// Usage: delete <filename>
func (server *FrontendServer) deleteFile(filename string) bool {
	conn := server.getLeaderConnection()
	if conn == nil {
		return false
	}
	defer conn.Close()

	if !common.SendMessage(conn, "DELETE_FILE "+filename) {
		return false
	}

	return common.GetOKMessage(conn)
}

// Usage: get <remote_filename> <local_filename>
func (server *FrontendServer) downloadFile(localFilename string, remoteFilename string) bool {
	connCache := NewConnectionCache()
	defer connCache.Close()

	file, err := os.Create(localFilename)
	if err != nil {
		log.Warn(err)
		return false
	}
	defer file.Close()

	leader := server.getLeaderConnection()
	if leader == nil {
		return false
	}
	defer leader.Close()

	request := fmt.Sprintf("DOWNLOAD_FILE %s\n", remoteFilename)
	log.Debug(request)
	if !common.SendMessage(leader, request) {
		return false
	}

	reader := bufio.NewReader(leader)
	fileSize := 0

	var startTime int64 = 0
	var endTime int64 = 0

	// log.Debug("Reading file block list")
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Warn(err)
			break
		}

		if fileSize == 0 {
			// log.Info("Starting download now...")
			startTime = time.Now().UnixNano()
		}

		line = line[:len(line)-1]
		log.Debug("Received:", line)
		tokens := strings.Split(line, " ")

		if len(tokens) == 1 {
			if tokens[0] == "ERROR" {
				log.Warn("ERROR")
				line, _ = reader.ReadString('\n') // Read error message on next line
				log.Warn(line)
				return false
			}
			break
		}

		if len(tokens) != 3 {
			log.Warn("Invalid number of tokens")
			return false
		}

		blockName := tokens[0]
		blockSize, err := strconv.Atoi(tokens[1])
		if err != nil {
			log.Warn(err)
			return false
		}

		replicas := strings.Split(tokens[2], ",")
		done := false

		for len(replicas) > 0 {
			i := rand.Intn(len(replicas))
			if downloadBlock(file, blockName, blockSize, replicas[i], connCache) {
				done = true
				break
			}

			// Remove current replica from list.
			// Then, retry download with another replica
			n := len(replicas)
			replicas[i], replicas[n-1] = replicas[n-1], replicas[i]
			replicas = replicas[:n-1]
		}

		if !done {
			log.Warn("Failed to download block:", blockName)
			return false
		}

		fileSize += blockSize
	}

	endTime = time.Now().UnixNano()
	log.Infof("Downloaded file %s (%d bytes) in %f seconds to %s\n", remoteFilename, fileSize, float64(endTime-startTime)*1e-9, localFilename)
	return common.SendMessage(leader, "OK")
}

// Download block from given replica and append data to file
// Returns number of bytes written to file, or -1 if failure
func downloadBlock(file *os.File, blockName string, blockSize int, replica string, connCache *ConnectionCache) bool {
	log.Debugf("Downloading block %s from %s\n", blockName, replica)
	conn := connCache.GetConnection(replica)
	if conn == nil {
		return false
	}

	request := fmt.Sprintf("DOWNLOAD %s\n", blockName)
	log.Debug(request)
	_, err := conn.Write([]byte(request))
	if err != nil {
		log.Warn(err)
		return false
	}

	buffer := make([]byte, blockSize)
	bufferSize := 0

	for bufferSize < blockSize {
		n, err := conn.Read(buffer[bufferSize:])
		if err != nil {
			log.Warn(err)
			return false
		}
		bufferSize += n
	}

	if bufferSize < blockSize {
		log.Warn("Insufficient bytes:", blockName)
		return false
	}

	log.Debugf("Received block %s (%d bytes) from %s\n", blockName, bufferSize, replica)
	_, err = file.Write(buffer[:bufferSize])
	if err != nil {
		log.Warn(err)
		return false
	}

	return true
}

// Usage: put <local_filename> <remote_filename>
// TODO: handle upload confirmation from master node
func (server *FrontendServer) uploadFile(localFilename string, remoteFilename string) bool {
	connCache := NewConnectionCache()
	defer connCache.Close()

	var blockName string
	var blockSize int
	var blockData []byte = make([]byte, common.BLOCK_SIZE)

	result := make(map[string][]string)

	info, err := os.Stat(localFilename)
	if err != nil {
		log.Warn(err)
		return false
	}

	fileSize := info.Size()
	file, err := os.Open(localFilename)
	if err != nil {
		log.Warn(err)
		return false
	}

	log.Debugf("To upload file %s with %d bytes (%d blocks)", localFilename, fileSize, common.GetNumFileBlocks(int64(fileSize)))
	defer file.Close()

	leader := server.getLeaderConnection()
	if leader == nil {
		return false
	}
	defer leader.Close()

	request := fmt.Sprintf("UPLOAD_FILE %s %d\n", remoteFilename, fileSize)
	log.Debug(request)
	if !common.SendMessage(leader, request) {
		return false
	}

	reader := bufio.NewReader(leader)

	line, err := reader.ReadString('\n')
	if err != nil {
		return false
	}

	if line[:len(line)-1] == "ERROR" {
		line, _ = reader.ReadString('\n') // Read error message on next line
		log.Warn("ERROR:", line)
		return false
	}

	var startTime int64 = time.Now().UnixNano()
	var endTime int64 = 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return false
		}

		line = line[:len(line)-1]
		log.Debug("Received:", line)

		tokens := strings.Split(line, " ")
		if tokens[0] == "END" {
			break
		}

		if len(tokens) < 2 {
			log.Warn("Illegal response")
			return false
		}

		replicas := strings.Split(tokens[1], ",")

		blockName = tokens[0]
		blockSize, err = file.Read(blockData)
		if err != nil {
			log.Warn(err)
			return false
		}

		info := &UploadInfo{server: leader, blockName: blockName, blockData: blockData, blockSize: blockSize, replicas: replicas}

		if !UploadBlockSync(info, connCache, result) {
			return false
		}

	}

	for block, replicas := range result {
		log.Info(block, replicas)
		if !common.SendMessage(leader, fmt.Sprintf("%s %s\n", block, strings.Join(replicas, ","))) {
			return false
		}
	}

	if !common.SendMessage(leader, "END") {
		return false
	}

	buffer := make([]byte, common.MIN_BUFFER_SIZE)
	n, err := leader.Read(buffer)
	if err != nil {
		return false
	}

	endTime = time.Now().UnixNano()
	fmt.Printf("Uploaded in %f seconds\n", float64(endTime-startTime)*1e-9)

	return string(buffer[:n-1]) == "UPLOAD_OK"
}

func UploadBlockSync(info *UploadInfo, connCache *ConnectionCache, result map[string][]string) bool {
	for _, replica := range info.replicas {
		log.Debugf("Uploading block %s (%d bytes) to %s\n", info.blockName, info.blockSize, replica)

		conn := connCache.GetConnection(replica)
		if conn == nil {
			return false
		}

		_, err := conn.Write([]byte(fmt.Sprintf("UPLOAD %s %d\n", info.blockName, info.blockSize)))
		if err != nil {
			return false
		}

		if !common.GetOKMessage(conn) {
			return false
		}

		if common.SendAll(conn, info.blockData[:info.blockSize], info.blockSize) < 0 {
			return false
		}

		if !common.GetOKMessage(conn) {
			return false
		}

		result[info.blockName] = append(result[info.blockName], replica)
	}

	return true
}
