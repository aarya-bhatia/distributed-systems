package frontend

import (
	"bufio"
	"cs425/common"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const MAX_RETRIES = 3
const INITIAL_BACKOFF_TIME = 400 * time.Millisecond

type UploadInfo struct {
	server    net.Conn
	blockSize int
	blockData []byte
	blockName string
	replicas  []string
}

// Upload with retry upto MAX_RETRIES times and return file replica list on success
func (server *FrontendServer) uploadFileWithRetry(localFilename string, remoteFilename string) ([]string, error) {
	stat, err := os.Stat(localFilename)
	if err != nil {
		return nil, err
	}

	fileSize := stat.Size()

	log.Debugf("To upload file %s with %d bytes (%d blocks)", localFilename, fileSize, common.GetNumFileBlocks(int64(fileSize)))

	for count, backoff := 0, INITIAL_BACKOFF_TIME; count < MAX_RETRIES; count, backoff = count+1, backoff*2 {

		// must have minimum number of replicas to upload
		if len(server.BackendServer.GetAliveNodes()) < common.REPLICA_FACTOR {
			return nil, errors.New("Replica nodes are offline")
		}

		file, err := os.Open(localFilename) // reopen file each try
		if err != nil {
			return nil, err
		}

		defer file.Close()

		leader := server.getLeaderConnection()
		if leader == nil {
			log.Warn("Leader offline!")
			log.Infof("Retrying upload after %d ms", backoff.Milliseconds())
			time.Sleep(backoff)
			continue
		}

		defer leader.Close()

		result, err := server.uploadFile(file, fileSize, remoteFilename, leader)

		if err != nil {
			common.SendMessage(leader, "ERROR")
			log.Warn(err.Error())
			log.Infof("Retrying upload after %d ms", backoff.Milliseconds())
			time.Sleep(backoff)
			continue
		}

		if !common.SendMessage(leader, "OK") {
			log.Warn("Failed to send OK to leader")
			log.Infof("Retrying upload after %d ms", backoff.Milliseconds())
			time.Sleep(backoff)
			continue
		}

		if !common.CheckMessage(leader, "UPLOAD_OK") {
			log.Warn("Failed to receive UPLOAD_OK from leader")
			log.Infof("Retrying upload after %d ms", backoff.Milliseconds())
			time.Sleep(backoff)
			continue
		}

		return result, nil // success!
	}

	return nil, errors.New("Max retires were exceeded")
}

// Reads replica list from leader and uploads the block to each replica
// On success, returns the replicas that received the blocks
func (server *FrontendServer) uploadFile(localFile *os.File, fileSize int64, remoteFilename string, leader net.Conn) ([]string, error) {
	connCache := NewConnectionCache()
	defer connCache.Close()

	blockData := make([]byte, common.BLOCK_SIZE)
	replicaSet := make(map[string]bool)

	request := fmt.Sprintf("UPLOAD_FILE %s %d\n", remoteFilename, fileSize)
	log.Debug(request)
	if !common.SendMessage(leader, request) {
		return nil, errors.New("Failed to send message")
	}

	reader := bufio.NewReader(leader)

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, errors.New("Failed to read message")
	}

	if line[:len(line)-1] == "ERROR" {
		line, _ = reader.ReadString('\n') // Read error message on next line
		return nil, errors.New(line)
	}

	// Read replica list from leader for each block until an 'END' message is read
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		line = line[:len(line)-1]

		tokens := strings.Split(line, " ")
		if tokens[0] == "END" {
			break
		}

		if len(tokens) < 2 {
			return nil, errors.New("Illegal response")
		}

		replicas := strings.Split(tokens[1], ",")
		blockName := tokens[0]
		blockSize, err := localFile.Read(blockData)
		if err != nil {
			return nil, err
		}

		info := &UploadInfo{
			server:    leader,
			blockName: blockName,
			blockData: blockData,
			blockSize: blockSize,
			replicas:  replicas,
		}

		if !uploadBlockSync(info, connCache) {
			return nil, errors.New("Failed to upload block")
		}

		for _, replica := range replicas {
			replicaSet[replica] = true
		}
	}

	replicas := []string{}

	for replica := range replicaSet {
		replicas = append(replicas, replica)
	}

	return replicas, nil
}

// Upload a single block to each replica
func uploadBlockSync(info *UploadInfo, connCache *ConnectionCache) bool {
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
	}

	return true
}
