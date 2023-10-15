package common

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hash"
	"hash/fnv"
	"net"
	"strings"
)

// Hash string s to an integer in [0,N)
func GetHash(s string, N int) int {
	var fnvHash hash.Hash32 = fnv.New32a()
	fnvHash.Write([]byte(s))
	hashValue := fnvHash.Sum32()
	return int(hashValue % uint32(N))
}

func GetBlockName(filename string, version int, blockNum int) string {
	return fmt.Sprintf("%s:%d:%d", filename, version, blockNum)
}

// Returns the number blocks for a file of given size
func GetNumFileBlocks(fileSize int64) int {
	n := int(fileSize / BLOCK_SIZE)
	if fileSize%BLOCK_SIZE > 0 {
		n += 1
	}
	return n
}

func Connect(hostname string, port int) net.Conn {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", hostname, port))
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func GetOKMessage(server net.Conn) bool {
	buffer := make([]byte, MIN_BUFFER_SIZE)
	n, err := server.Read(buffer)
	if err != nil {
		log.Println(err)
		return false
	}

	message := string(buffer[:n])

	if strings.Index(message, "OK") != 0 {
		log.Warn(message)
		return false
	}

	return true
}

// Returns true if all bytes are uploaded to network
func SendAll(conn net.Conn, buffer []byte, count int) int {
	sent := 0
	n := -1

	for sent < count && n != 0 {
		n, err := conn.Write(buffer[sent:count])
		if err != nil {
			log.Println(err)
			return -1
		}
		sent += n
	}

	log.Debugf("Sent %d bytes to %s\n", sent, conn.RemoteAddr())

	return sent
}
