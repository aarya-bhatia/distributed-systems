package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	ENV                     = "DEV"
	DEFAULT_PORT            = 5000
	BLOCK_SIZE              = 16
	MIN_BUFFER_SIZE         = 1024
	DEFAULT_SERVER_HOSTNAME = "localhost"
)

type Node struct {
	ID       int
	Hostname string
	Port     int
}

var cluster = []Node{
	{1, "fa23-cs425-0701.cs.illinois.edu", DEFAULT_PORT},
	{2, "fa23-cs425-0702.cs.illinois.edu", DEFAULT_PORT},
	{3, "fa23-cs425-0703.cs.illinois.edu", DEFAULT_PORT},
	{4, "fa23-cs425-0704.cs.illinois.edu", DEFAULT_PORT},
	{5, "fa23-cs425-0705.cs.illinois.edu", DEFAULT_PORT},
	{6, "fa23-cs425-0706.cs.illinois.edu", DEFAULT_PORT},
	{7, "fa23-cs425-0707.cs.illinois.edu", DEFAULT_PORT},
	{8, "fa23-cs425-0708.cs.illinois.edu", DEFAULT_PORT},
	{9, "fa23-cs425-0709.cs.illinois.edu", DEFAULT_PORT},
	{10, "fa23-cs425-0710.cs.illinois.edu", DEFAULT_PORT},
}

var nodes = []Node{
	{1, "localhost", 5000},
	{2, "localhost", 5001},
	{3, "localhost", 5002},
	{4, "localhost", 5003},
	{5, "localhost", 5004},
}

func downloadFile(localFilename string, remoteFilename string) bool {
	server := connectToServer(DEFAULT_SERVER_HOSTNAME, DEFAULT_PORT)
	defer server.Close()

	file, err := os.OpenFile(localFilename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0640)
	if err != nil {
		return false
	}

	defer file.Close()

	message := fmt.Sprintf("DOWNLOAD_FILE %s\n", remoteFilename)

	if !SendAll(server, []byte(message), len(message)) {
		return false
	}

	if !getOK(server) {
		return false
	}

	buffer := make([]byte, BLOCK_SIZE)
	bytesRead := 0

	for {
		n, err := server.Read(buffer)
		if err == io.EOF {
			log.Debug("EOF")
			break
		} else if err != nil {
			return false
		}

		_, err = file.Write(buffer[:n])
		if err != nil {
			return false
		}

		bytesRead += n
	}

	log.Infof("Downloaded file %s: %d bytes\n", localFilename, bytesRead)

	return true
}

func uploadFile(localFilename string, remoteFilename string) bool {
	server := connectToServer(DEFAULT_SERVER_HOSTNAME, DEFAULT_PORT)
	defer server.Close()

	info, err := os.Stat(localFilename)
	if err != nil {
		return false
	}

	fileSize := info.Size
	file, err := os.Open(localFilename)
	if err != nil {
		return false
	}

	defer file.Close()

	message := fmt.Sprintf("UPLOAD_FILE %s %d\n", remoteFilename, fileSize())

	if !SendAll(server, []byte(message), len(message)) {
		return false
	}

	if !getOK(server) {
		return false
	}

	buffer := make([]byte, BLOCK_SIZE)

	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			return false
		}

		if !SendAll(server, buffer, n) {
			return false
		}
	}

	if !getOK(server) {
		return false
	}

	return true
}

func printUsage() {
	log.Info("Usage:")
	log.Info("Upload file: put local_filename remote_filename")
	log.Info("Download file: get remote_filename local_filename")
	log.Info("Query file: query remote_filename")
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		tokens := strings.Split(line, " ")
		verb := tokens[0]
		if verb == "help" {
			printUsage()
		} else if verb == "put" {
			if len(tokens) != 3 {
				printUsage()
			} else if !uploadFile(tokens[1], tokens[2]) {
				log.Warn("Failed to upload file")
			} else {
				log.Println("Success!")
			}
		} else if verb == "get" {
			if len(tokens) != 3 {
				printUsage()
			} else if !downloadFile(tokens[2], tokens[1]) {
				log.Warn("Failed to download file ", tokens[2])
			} else {
				log.Println("Success!")
			}
		} else {
			log.Warn("Unknown command ", verb)
			printUsage()
		}
	}
}
