package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
)

// This helps reuse same TCP connection to download multiple blocks from a replica
// Maps replcia address to connection
var connections map[string]net.Conn = make(map[string]net.Conn)

func getConnection(addr string) net.Conn {
	if _, ok := connections[addr]; !ok {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			Log.Warn("Failed to connect to", addr)
			return nil
		}

		Log.Info("Connected to", addr)
		connections[addr] = conn
	}

	return connections[addr]
}

func closeConnections() {
	for addr, conn := range connections {
		Log.Info("Closing connection with", addr)
		conn.Close()
	}
}

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
		replica := replicas[rand.Intn(len(replicas))] // choose one random replica

		ret := downloadBlock(file, blockName, blockSize, replica)
		if ret <= 0 {
			return false
		}

		fileSize += ret
	}

	server.Write([]byte("OK\n"))
	Log.Infof("Downloaded file %s with %d bytes\n", localFilename, fileSize)
	return true
}

// Download block from given replica and append data to file
// Returns number of bytes written to file, or -1 if failure
func downloadBlock(file *os.File, blockName string, blockSize int, replica string) int {
	Log.Debugf("Downloading block %s from %s\n", blockName, replica)
	conn := getConnection(replica)
	if conn == nil {
		return -1
	}

	request := fmt.Sprintf("DOWNLOAD %s\n", blockName)
	Log.Debug(request)
	_, err := conn.Write([]byte(request))
	if err != nil {
		Log.Warn(err)
		return -1
	}

	buffer := make([]byte, blockSize)
	bufferSize := 0

	for bufferSize < blockSize {
		n, err := conn.Read(buffer[bufferSize:])
		if err != nil {
			Log.Warn(err)
			return -1
		}
		bufferSize += n
	}

	if bufferSize < blockSize {
		Log.Warn("Insufficient bytes received for block", blockName)
		return -1
	}

	Log.Debugf("Received block %s (%d bytes) from %s\n", blockName, bufferSize, replica)
	n, err := file.Write(buffer[:bufferSize])
	if err != nil {
		Log.Warn(err)
		return -1
	}

	return n
}
