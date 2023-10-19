package main

import (
	"bufio"
	"cs425/common"
	"fmt"
	"net"
	"os"
	"strings"
)

func UploadFile(localFilename string, remoteFilename string) bool {
	server := Connect()
	defer server.Close()

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

	if common.SendAll(server, []byte(message), len(message)) < 0 {
		return false
	}

	fileBlock := make([]byte, common.BLOCK_SIZE)

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

		blockName := tokens[0]
		replicas := tokens[1]
		replica := strings.Split(replicas, ",")[0]

		Log.Debug("Uploading block ", blockName)

		blockSize, err := file.Read(fileBlock)
		if err != nil {
			Log.Warn(err)
			return false
		}

		conn, err := net.Dial("tcp", replica)
		if err != nil {
			return false
		}

		_, err = conn.Write([]byte(fmt.Sprintf("UPLOAD %s %d\n", blockName, blockSize)))
		if err != nil {
			return false
		}

		if !common.GetOKMessage(conn) {
			return false
		}

		if common.SendAll(conn, fileBlock[:blockSize], blockSize) < 0 {
			return false
		}

		_, err = server.Write([]byte(fmt.Sprintf("%s %s\n", blockName, replica)))
		if err != nil {
			return false
		}
	}

	return true
}
