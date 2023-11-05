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

type UploadInfo struct {
	server    net.Conn
	blockSize int
	blockData []byte
	blockName string
	replicas  []string
}

func (server *FrontendServer) uploadFileWithRetry(localFilename string, remoteFilename string) ([]string, error) {
	backoff := 200 * time.Millisecond

	stat, err := os.Stat(localFilename)
	if err != nil {
		return nil, err
	}

	fileSize := stat.Size()

	log.Debugf("To upload file %s with %d bytes (%d blocks)", localFilename, fileSize, common.GetNumFileBlocks(int64(fileSize)))

	count := 0

	// must have minimum number of replicas to upload
	for count < MAX_RETRIES && len(server.BackendServer.GetAliveNodes()) >= common.REPLICA_FACTOR {
		file, err := os.Open(localFilename) // reopen file each time
		if err != nil {
			return nil, err
		}
		defer file.Close()

		result, err := server.uploadFile(file, fileSize, remoteFilename)
		if err == nil {
			return result, nil
		}

		log.Warn(err.Error())

		time.Sleep(backoff)
		backoff *= 2
		log.Infof("Retrying upload after %d ms", backoff.Milliseconds())
		count++
	}

	if count >= MAX_RETRIES {
		return nil, errors.New("Max retires were exceeded")
	}

	return nil, errors.New("Replica nodes are offline")
}

func (server *FrontendServer) uploadFile(localFile *os.File, fileSize int64, remoteFilename string) ([]string, error) {
	connCache := NewConnectionCache()
	defer connCache.Close()

	replicaSet := make(map[string]bool)

	leader := server.getLeaderConnection()
	if leader == nil {
		return nil, errors.New("Leader is offline")
	}
	defer leader.Close()

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

	startTime := time.Now().UnixNano()

	blockData := make([]byte, common.BLOCK_SIZE)

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

		info := &UploadInfo{server: leader, blockName: blockName, blockData: blockData, blockSize: blockSize, replicas: replicas}

		if !uploadBlockSync(info, connCache) {
			return nil, errors.New("Failed to upload block")
		}

		for _, replica := range replicas {
			replicaSet[replica] = true
		}
	}

	if !common.SendMessage(leader, "OK") {
		return nil, errors.New("Failed to send OK to leader")
	}

	if !common.CheckMessage(leader, "UPLOAD_OK") {
		return nil, errors.New("Failed to receive UPLOAD_OK from leader")
	}

	endTime := time.Now().UnixNano()
	fmt.Printf("Uploaded in %f seconds\n", float64(endTime-startTime)*1e-9)

	replicas := []string{}
	for replica := range replicaSet {
		replicas = append(replicas, replica)
	}

	return replicas, nil
}

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
