package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"

	"mp3/common"
)

func DownloadFile(localFilename string, remoteFilename string) bool {
	server := common.Connect(common.DEFAULT_SERVER_HOSTNAME, common.DEFAULT_PORT)
	defer server.Close()

	file, err := os.Create(localFilename)
	if err != nil {
		log.Println(err)
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
			log.Debug("EOF")
			break
		} else if err != nil {
			log.Println(err)
			return false
		}

		_, err = file.Write(buffer[:n])
		if err != nil {
			log.Println(err)
			return false
		}

		bytesRead += n
	}

	log.Infof("Downloaded file %s with %d bytes\n", localFilename, bytesRead)
	return true
}

func UploadFile(localFilename string, remoteFilename string) bool {
	server := common.Connect(common.DEFAULT_SERVER_HOSTNAME, common.DEFAULT_PORT)
	defer server.Close()

	info, err := os.Stat(localFilename)
	if err != nil {
		log.Println(err)
		return false
	}

	fileSize := info.Size()
	file, err := os.Open(localFilename)
	if err != nil {
		log.Println(err)
		return false
	}

	log.Debugf("Uploading file %s with %d bytes (%d blocks)", localFilename, fileSize, common.GetNumFileBlocks(int64(fileSize)))

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
			log.Println(err)
			return false
		}

		if common.SendAll(server, buffer, n) < 0 {
			return false
		}

		log.Debugf("Sent block with %d bytes", n)
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

	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})

	log.SetReportCaller(true)
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)

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
				log.Warn("Failed to upload file")
			} else {
				log.Println("Success!")
			}
		} else if verb == "get" {
			if len(tokens) != 3 {
				printUsage()
			} else if !DownloadFile(tokens[2], tokens[1]) {
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
