package main

import (
	"bufio"
	"cs425/common"
	"fmt"
	"os"
	"strings"
)

const MIN_REPLICAS = 1

func UploadFile(localFilename string, remoteFilename string) bool {
	var blockName string
	var blockSize int
	var blockData []byte = make([]byte, common.BLOCK_SIZE)

	info, err := os.Stat(localFilename)
	if err != nil {
		Log.Warn(err)
		return false
	}

	fileSize := info.Size()
	file, err := os.Open(localFilename)
	if err != nil {
		Log.Warn(err)
		return false
	}

	Log.Debugf("Uploading file %s with %d bytes (%d blocks)", localFilename, fileSize, common.GetNumFileBlocks(int64(fileSize)))

	defer file.Close()

	message := fmt.Sprintf("UPLOAD_FILE %s %d\n", remoteFilename, fileSize)

	server := Connect()
	defer server.Close()
	// TODO: Ask for leader node

	if common.SendAll(server, []byte(message), len(message)) < 0 {
		return false
	}

	reader := bufio.NewReader(server)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			Log.Warn(err)
			break
		}

		line = line[:len(line)-1]
		Log.Debug("Received:", line)
		tokens := strings.Split(line, " ")
		if len(tokens) != 2 {
			break
		}

		replicas := strings.Split(tokens[1], ",")

		blockName = tokens[0]
		blockSize, err = file.Read(blockData)
		if err != nil {
			Log.Warn(err)
			return false
		}

		info := &UploadInfo{server: server, blockName: blockName, blockData: blockData, blockSize: blockSize, replicas: replicas}

		if !StartFastUpload(info) {
			return false
		}

		/* if !UploadBlockSync(server, replicas, blockData, blockSize) {
			return false
		} */
	}

	server.Write([]byte("END\n"))
	return true
}

// func UploadBlockSync(server net.Conn, replicas []string, fileBlock []byte, blockSize int) bool {
// 	for _, replica := range replicas {
// 		Log.Debugf("Uploading block %s (%d bytes) to %s\n", blockName, blockSize, replica)
//
// 		conn, err := net.Dial("tcp", replica)
// 		if err != nil {
// 			return false
// 		}
//
// 		defer conn.Close()
// 		Log.Debug("Connected to ", replica)
//
// 		_, err = conn.Write([]byte(fmt.Sprintf("UPLOAD %s %d\n", blockName, blockSize)))
// 		if err != nil {
// 			return false
// 		}
//
// 		Log.Debug("Sent upload block request to ", replica)
//
// 		if !common.GetOKMessage(conn) {
// 			return false
// 		}
//
// 		Log.Debug("Got OK from ", replica)
//
// 		if common.SendAll(conn, fileBlock[:blockSize], blockSize) < 0 {
// 			return false
// 		}
//
// 		Log.Debugf("Sent block (%d bytes) to %s\n", blockSize, replica)
//
// 		_, err = server.Write([]byte(fmt.Sprintf("%s %s\n", blockName, replica)))
// 		if err != nil {
// 			return false
// 		}
//
// 		Log.Debug("Sent update to server")
// 	}
//
// 	return true
// }
