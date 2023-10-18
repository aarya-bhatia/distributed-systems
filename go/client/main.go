package main

import (
	"bufio"
	"cs425/common"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

var Log = common.Log

func Connect() net.Conn {
	for _, node := range common.Cluster {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", node.Hostname, node.TCPPort))
		if err == nil {
			return conn
		}
	}

	Log.Fatal("All nodes are offline")
	return nil
}

func DownloadFile(localFilename string, remoteFilename string) bool {
	server := Connect()
	defer server.Close()

	file, err := os.Create(localFilename)
	if err != nil {
		Log.Warn(err)
		return false
	}

	defer file.Close()
	message := fmt.Sprintf("DOWNLOAD_FILE %s\n", remoteFilename)

	if common.SendAll(server, []byte(message), len(message)) < 0 {
		return false
	}

	if !common.GetOKMessage(server) {
		return false
	}

	buffer := make([]byte, common.BLOCK_SIZE)
	bytesRead := 0

	for {
		n, err := server.Read(buffer)
		if err == io.EOF {
			Log.Debug("EOF")
			break
		} else if err != nil {
			Log.Warn(err)
			return false
		}

		_, err = file.Write(buffer[:n])
		if err != nil {
			Log.Warn(err)
			return false
		}

		bytesRead += n
	}

	Log.Infof("Downloaded file %s with %d bytes\n", localFilename, bytesRead)
	return true
}

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

	if !common.GetOKMessage(server) {
		return false
	}

	buffer := make([]byte, common.BLOCK_SIZE)

	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			Log.Warn(err)
			return false
		}

		Log.Debugf("Read %d bytes\n", n)

		if common.SendAll(server, buffer, n) < 0 {
			return false
		}

		Log.Debugf("Sent %d bytes\n", n)
	}

	if !common.GetOKMessage(server) {
		return false
	}

	return true
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("Upload file: put local_filename remote_filename")
	fmt.Println("Download file: get remote_filename local_filename")
	fmt.Println("Query file: query remote_filename")
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)

	if os.Getenv("prod") == "true" {
		common.Cluster = common.ProdCluster
	} else {
		common.Cluster = common.LocalCluster
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		tokens := strings.Split(line, " ")
		verb := tokens[0]
		if verb == "help" {
			printUsage()
		} else if verb == "put" {
			if len(tokens) != 3 {
				printUsage()
				continue
			}

			startTime := time.Now().UnixNano()

			if !UploadFile(tokens[1], tokens[2]) {
				Log.Warn("Failed to upload file")
			} else {
				Log.Debug("Success!")
				endTime := time.Now().UnixNano()
				elapsed := endTime - startTime
				fmt.Println("Upload time (sec): ", float64(elapsed)*1e-9)
			}

		} else if verb == "get" {
			if len(tokens) != 3 {
				printUsage()
				continue
			}

			startTime := time.Now().UnixNano()

			if !DownloadFile(tokens[2], tokens[1]) {
				Log.Warn("Failed to download file ", tokens[2])
			} else {
				Log.Debug("Success!")
				endTime := time.Now().UnixNano()
				elapsed := endTime - startTime
				fmt.Println("Download time (sec): ", float64(elapsed)*1e-9)
			}
		} else {
			Log.Warn("Unknown command ", verb)
			printUsage()
		}
	}
}
