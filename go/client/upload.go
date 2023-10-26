package main

import (
	"bufio"
	"cs425/common"
	"fmt"
	"net"
	"os"
	"strings"
)

const MIN_REPLICAS = 1

// TODO: handle upload confirmation from master node
func UploadFile(localFilename string, remoteFilename string) bool {
	var blockName string
	var blockSize int
	var blockData []byte = make([]byte, common.BLOCK_SIZE)

	connections := make(map[string]net.Conn)
	result := make(map[string][]string)

	defer func() {
		for addr, connection := range connections {
			Log.Info("Closing connection with", addr)
			connection.Close()
		}
	}()

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

	line, err := reader.ReadString('\n')
	if err != nil {
		return false
	}

	if line[:len(line)-1] == "ERROR" {
		line, _ = reader.ReadString('\n') // Read error message on next line
		Log.Warn("ERROR", line)
		return false
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return false
		}

		line = line[:len(line)-1]
		Log.Debug("Received:", line)

		tokens := strings.Split(line, " ")
		if tokens[0] == "END" {
			break
		}

		if len(tokens) < 2 {
			Log.Warn("Illegal response")
			return false
		}

		replicas := strings.Split(tokens[1], ",")

		blockName = tokens[0]
		blockSize, err = file.Read(blockData)
		if err != nil {
			Log.Warn(err)
			return false
		}

		info := &UploadInfo{server: server, blockName: blockName, blockData: blockData, blockSize: blockSize, replicas: replicas}

		// if !StartFastUpload(info) {
		// 	return false
		// }

		if !UploadBlockSync(info, connections, result) {
			return false
		}

	}

	for block, replicas := range result {
		Log.Info("== RESULT", block, replicas)

		if !common.SendMessage(server, fmt.Sprintf("%s %s\n", block, strings.Join(replicas, ","))) {
			return false
		}

		Log.Debug("Sent update to server for block", block)
	}

	if !common.SendMessage(server, "END") {
		return false
	}

	buffer := make([]byte, common.MIN_BUFFER_SIZE)
	n, err := server.Read(buffer)
	if err != nil {
		return false
	}

	return string(buffer[:n-1]) == "UPLOAD_OK"
}

func UploadBlockSync(info *UploadInfo, connections map[string]net.Conn, result map[string][]string) bool {
	for _, replica := range info.replicas {
		Log.Debugf("Uploading block %s (%d bytes) to %s\n", info.blockName, info.blockSize, replica)

		if _, ok := connections[replica]; !ok {
			conn, err := net.Dial("tcp", replica)
			if err != nil {
				return false
			}
			Log.Debug("Connected to ", replica)
			connections[replica] = conn
		}

		conn := connections[replica]

		_, err := conn.Write([]byte(fmt.Sprintf("UPLOAD %s %d\n", info.blockName, info.blockSize)))
		if err != nil {
			return false
		}

		Log.Debug("Sent upload block request to ", replica)

		if !common.GetOKMessage(conn) {
			return false
		}

		Log.Debug("Got OK from ", replica)

		if common.SendAll(conn, info.blockData[:info.blockSize], info.blockSize) < 0 {
			return false
		}

		Log.Debugf("Sent block (%d bytes) to %s\n", info.blockSize, replica)

		if !common.GetOKMessage(conn) {
			return false
		}

		Log.Debug("Got OK from ", replica)

		result[info.blockName] = append(result[info.blockName], replica)
	}

	return true
}
