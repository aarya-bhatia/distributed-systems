package main

import (
	"fmt"
	"net"
	"os"

	log "github.com/sirupsen/logrus"
	"strings"
)

func connectToServer(hostname string, port int) net.Conn {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", hostname, port))
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func getOK(server net.Conn) bool {
	buffer := make([]byte, MIN_BUFFER_SIZE)
	n, err := server.Read(buffer)
	if err != nil {
		return false
	}

	message := string(buffer[:n])
	if strings.Index(message, "OK") != 0 {
		return false
	} else {
		log.Warn(message)
	}

	return true
}

// Returns true if all bytes are uploaded to network
func SendAll(conn net.Conn, buffer []byte, count int) bool {
	sent := 0

	for sent < count {
		n, err := conn.Write(buffer[sent:count])
		if err != nil {
			return false
		}
		sent += n
	}

	return true
}

// Returns the number blocks for a file of given size
func GetNumFileBlocks(fileSize int64) int {
	n := int(fileSize / BLOCK_SIZE)
	if fileSize%BLOCK_SIZE > 0 {
		n += 1
	}
	return n
}

func SplitFileIntoBlocks(filename string, outputDirectory string) bool {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return false
	}

	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return false
	}

	fileSize := info.Size()
	numBlocks := GetNumFileBlocks(fileSize)
	buffer := make([]byte, BLOCK_SIZE)

	for i := 0; i < numBlocks; i++ {
		n, err := f.Read(buffer)
		if err != nil {
			return false
		}

		outputFilename := fmt.Sprintf("%s/%s_block%d", outputDirectory, filename, i)
		outputFile, err := os.OpenFile(outputFilename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0640)
		if err != nil {
			return false
		}

		_, err = outputFile.Write(buffer)
		if err != nil {
			return false
		}

		outputFile.Close()

		if n < BLOCK_SIZE {
			break
		}
	}

	return true
}
