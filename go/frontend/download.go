package frontend

import (
	"bufio"
	"cs425/common"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

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

func (server *FrontendServer) downloadFileWithRetry(localFilename string, remoteFilename string) bool {
	// TODO
	// backoff := 200 * time.Millisecond
	//
	// for len(server.BackendServer.GetAliveNodes()) >= common.REPLICA_FACTOR {
	// 	if server.uploadFile(localFilename, remoteFilename) {
	// 		return true
	// 	}
	//
	// 	time.Sleep(backoff)
	// 	backoff *= 2
	// }
	//
	return false
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
