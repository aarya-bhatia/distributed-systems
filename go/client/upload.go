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

		blockSize, err := file.Read(fileBlock)
		if err != nil {
			Log.Warn(err)
			return false
		}

		for _, replica := range strings.Split(replicas, ",") {
			Log.Debugf("Uploading block %s (%d bytes) to %s\n", blockName, blockSize, replica)

			conn, err := net.Dial("tcp", replica)
			if err != nil {
				return false
			}

			defer conn.Close()
			Log.Debug("Connected to ", replica)

			_, err = conn.Write([]byte(fmt.Sprintf("UPLOAD %s %d\n", blockName, blockSize)))
			if err != nil {
				return false
			}

			Log.Debug("Sent upload block request to ", replica)

			if !common.GetOKMessage(conn) {
				return false
			}

			Log.Debug("Got OK from ", replica)

			if common.SendAll(conn, fileBlock[:blockSize], blockSize) < 0 {
				return false
			}

			Log.Debugf("Sent block (%d bytes) to %s\n", blockSize, replica)

			_, err = server.Write([]byte(fmt.Sprintf("%s %s\n", blockName, replica)))
			if err != nil {
				return false
			}

			Log.Debug("Sent update to server")
		}
	}

	server.Write([]byte("END\n"))
	return true
}
