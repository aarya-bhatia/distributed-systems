package main

import (
	"bufio"
	"cs425/common"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
)

func DownloadFile(localFilename string, remoteFilename string) bool {
	file, err := os.Create(localFilename)
	if err != nil {
		Log.Warn(err)
		return false
	}
	defer file.Close()

	server := Connect()
	defer server.Close()

	Log.Debug("Sending download file request")
	server.Write([]byte(fmt.Sprintf("DOWNLOAD_FILE %s\n", remoteFilename)))

	reader := bufio.NewReader(server)
	fileSize := 0

	Log.Debug("Reading file block list")
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

			if tokens[0] == "ERROR" {
				Log.Warn("ERROR")
				line, _ = reader.ReadString('\n') // Read error message on next line
				Log.Warn(line)
				return false
			}

			break
		}

		blockName := tokens[0]
		replicas := strings.Split(tokens[1], ",")
		replica := replicas[rand.Intn(len(replicas))]

		Log.Debugf("Downloading block %s from %s\n", blockName, replica)

		conn, err := net.Dial("tcp", replica)
		if err != nil {
			Log.Warn(err)
			return false
		}

		defer conn.Close()
		Log.Debug("Connected to ", replica)

		Log.Debug("Sending download request to replica ", replica)
		conn.Write([]byte(fmt.Sprintf("DOWNLOAD %s\n", blockName)))

		buffer := make([]byte, common.BLOCK_SIZE)
		bufferSize := 0
		for bufferSize < len(buffer) {
			n, err := conn.Read(buffer[bufferSize:])
			if err == io.EOF {
				break
			} else if err != nil {
				Log.Warn(err)
				return false
			}
			if n == 0 {
				break
			}
			bufferSize += n
		}
		Log.Debugf("Received block %s (%d bytes) from %s\n", blockName, bufferSize, replica)
		n, err := file.Write(buffer[:bufferSize])
		if err != nil {
			Log.Warn(err)
			return false
		}
		fileSize += n
	}

	server.Write([]byte("OK\n"))
	Log.Infof("Downloaded file %s with %d bytes\n", localFilename, fileSize)
	return true
}
