package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"cs425/common"
)

var Logger *log.Logger
var hostname string
var port int

func DownloadFile(localFilename string, remoteFilename string) bool {
	server := common.Connect(hostname, port)
	defer server.Close()

	file, err := os.Create(localFilename)
	if err != nil {
		Logger.Println(err)
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
			Logger.Debug("EOF")
			break
		} else if err != nil {
			Logger.Println(err)
			return false
		}

		_, err = file.Write(buffer[:n])
		if err != nil {
			Logger.Println(err)
			return false
		}

		bytesRead += n
	}

	Logger.Infof("Downloaded file %s with %d bytes\n", localFilename, bytesRead)
	return true
}

func UploadFile(localFilename string, remoteFilename string) bool {
	server := common.Connect(hostname, port)
	defer server.Close()

	info, err := os.Stat(localFilename)
	if err != nil {
		Logger.Println(err)
		return false
	}

	fileSize := info.Size()
	file, err := os.Open(localFilename)
	if err != nil {
		Logger.Println(err)
		return false
	}

	Logger.Debugf("Uploading file %s with %d bytes (%d blocks)", localFilename, fileSize, common.GetNumFileBlocks(int64(fileSize)))

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
			Logger.Println(err)
			return false
		}

		if common.SendAll(server, buffer, n) < 0 {
			return false
		}

		Logger.Debugf("Sent block with %d bytes", n)
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
	if len(os.Args) != 3 {
		Logger.Fatal("Usage: go run ./client hostname port")
	}

	scanner := bufio.NewScanner(os.Stdin)

	Logger = common.NewLogger(log.DebugLevel, true, true)

	hostname = os.Args[1]
	port, _ = strconv.Atoi(os.Args[2])

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		tokens := strings.Split(line, " ")
		verb := tokens[0]
		if verb == "help" {
			printUsage()
		} else if verb == "put" {
			if len(tokens) != 3 {
				printUsage()
			} else if !UploadFile(tokens[1], tokens[2]) {
				Logger.Warn("Failed to upload file")
			} else {
				Logger.Println("Success!")
			}
		} else if verb == "get" {
			if len(tokens) != 3 {
				printUsage()
			} else if !DownloadFile(tokens[2], tokens[1]) {
				Logger.Warn("Failed to download file ", tokens[2])
			} else {
				Logger.Println("Success!")
			}
		} else {
			Logger.Warn("Unknown command ", verb)
			printUsage()
		}
	}
}
