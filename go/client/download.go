package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func DownloadFile(localFilename string, remoteFilename string) bool {
	defer closeConnections()

	file, err := os.Create(localFilename)
	if err != nil {
		Log.Warn(err)
		return false
	}
	defer file.Close()

	server := Connect()
	defer server.Close()

	request := fmt.Sprintf("DOWNLOAD_FILE %s\n", remoteFilename)
	Log.Debug(request)
	_, err = server.Write([]byte(request))
	if err != nil {
		return false
	}

	reader := bufio.NewReader(server)
	fileSize := 0

	var startTime int64 = 0
	var endTime int64 = 0

	Log.Debug("Reading file block list")
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			Log.Warn(err)
			break
		}

		if fileSize == 0 {
			Log.Info("Starting download now...")
			startTime = time.Now().UnixNano()
		}

		line = line[:len(line)-1]
		Log.Debug("Received:", line)
		tokens := strings.Split(line, " ")

		if len(tokens) == 1 {
			if tokens[0] == "ERROR" {
				Log.Warn("ERROR")
				line, _ = reader.ReadString('\n') // Read error message on next line
				Log.Warn(line)
				return false
			}
			break
		}

		if len(tokens) != 3 {
			Log.Warn("Invalid number of tokens")
			return false
		}

		blockName := tokens[0]
		blockSize, err := strconv.Atoi(tokens[1])
		if err != nil {
			Log.Warn(err)
			return false
		}

		replicas := strings.Split(tokens[2], ",")
		done := false

		for len(replicas) > 0 {
			i := rand.Intn(len(replicas))
			if downloadBlock(file, blockName, blockSize, replicas[i]) {
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
			Log.Warn("Failed to download block", blockName)
			return false
		}

		fileSize += blockSize
	}

	endTime = time.Now().UnixNano()
	fmt.Printf("Downloaded in %f seconds", float64(endTime-startTime)*1e-9)

	server.Write([]byte("OK\n"))
	Log.Infof("Downloaded file %s with %d bytes\n", localFilename, fileSize)
	return true
}

// Download block from given replica and append data to file
// Returns number of bytes written to file, or -1 if failure
func downloadBlock(file *os.File, blockName string, blockSize int, replica string) bool {
	Log.Debugf("Downloading block %s from %s\n", blockName, replica)
	conn := getConnection(replica)
	if conn == nil {
		return false
	}

	request := fmt.Sprintf("DOWNLOAD %s\n", blockName)
	Log.Debug(request)
	_, err := conn.Write([]byte(request))
	if err != nil {
		Log.Warn(err)
		return false
	}

	buffer := make([]byte, blockSize)
	bufferSize := 0

	for bufferSize < blockSize {
		n, err := conn.Read(buffer[bufferSize:])
		if err != nil {
			Log.Warn(err)
			return false
		}
		bufferSize += n
	}

	if bufferSize < blockSize {
		Log.Warn("Insufficient bytes received for block", blockName)
		return false
	}

	Log.Debugf("Received block %s (%d bytes) from %s\n", blockName, bufferSize, replica)
	_, err = file.Write(buffer[:bufferSize])
	if err != nil {
		Log.Warn(err)
		return false
	}

	return true
}
