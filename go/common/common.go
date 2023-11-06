package common

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hash"
	"hash/fnv"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

const (
	MAX_NODES       = 10
	BLOCK_SIZE      = 16 * 1024 * 1024 // 16 MB
	MIN_BUFFER_SIZE = 1024
	REPLICA_FACTOR  = 4

	POLL_INTERVAL      = 1 * time.Second
	REBALANCE_INTERVAL = 3 * time.Second

	JOIN_RETRY_TIMEOUT = time.Second * 5

	NODES_PER_ROUND = 4 // Number of random peers to send gossip every round

	GOSSIP_PROTOCOL            = 0
	GOSPSIP_SUSPICION_PROTOCOL = 1

	NODE_ALIVE     = 0
	NODE_SUSPECTED = 1
	NODE_FAILED    = 2

	DEFAULT_FRONTEND_PORT = 4000
	DEFAULT_BACKEND_PORT  = 5000
	DEFAULT_UDP_PORT      = 6000
)

type Node struct {
	ID           int
	Hostname     string
	UDPPort      int
	TCPPort      int
	FrontendPort int
}

type FileEntry struct {
	Name string
	Size int64
}

type Notifier interface {
	HandleNodeJoin(node *Node)
	HandleNodeLeave(node *Node)
}

var ProdCluster = []Node{
	{1, "fa23-cs425-0701.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_BACKEND_PORT, DEFAULT_FRONTEND_PORT},
	{2, "fa23-cs425-0702.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_BACKEND_PORT, DEFAULT_FRONTEND_PORT},
	{3, "fa23-cs425-0703.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_BACKEND_PORT, DEFAULT_FRONTEND_PORT},
	{4, "fa23-cs425-0704.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_BACKEND_PORT, DEFAULT_FRONTEND_PORT},
	{5, "fa23-cs425-0705.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_BACKEND_PORT, DEFAULT_FRONTEND_PORT},
	{6, "fa23-cs425-0706.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_BACKEND_PORT, DEFAULT_FRONTEND_PORT},
	{7, "fa23-cs425-0707.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_BACKEND_PORT, DEFAULT_FRONTEND_PORT},
	{8, "fa23-cs425-0708.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_BACKEND_PORT, DEFAULT_FRONTEND_PORT},
	{9, "fa23-cs425-0709.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_BACKEND_PORT, DEFAULT_FRONTEND_PORT},
	{10, "fa23-cs425-0710.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_BACKEND_PORT, DEFAULT_FRONTEND_PORT},
}

var LocalCluster = []Node{
	{1, "localhost", 6001, 5001, 4001},
	{2, "localhost", 6002, 5002, 4002},
	{3, "localhost", 6003, 5003, 4003},
	{4, "localhost", 6004, 5004, 4004},
	{5, "localhost", 6005, 5005, 4005},
	{6, "localhost", 6006, 5006, 4006},
	{7, "localhost", 6007, 5007, 4007},
	{8, "localhost", 6008, 5008, 4008},
	{9, "localhost", 6009, 5009, 4009},
	{10, "localhost", 6010, 5010, 4010},
}

var Cluster []Node = LocalCluster

func GetNodeByAddress(hostname string, udpPort int) *Node {
	for _, node := range Cluster {
		if node.Hostname == hostname && node.UDPPort == udpPort {
			return &node
		}
	}
	return nil
}

// Hash string s to an integer in [0,N)
func GetHash(s string, N int) int {
	var fnvHash hash.Hash32 = fnv.New32a()
	fnvHash.Write([]byte(s))
	hashValue := fnvHash.Sum32()
	return int(hashValue % uint32(N))
}

// Returns block name using the convention filename:version:blockNumber
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

// Sends message to given connection and returns true if successful
func SendMessage(conn net.Conn, message string) bool {
	if strings.LastIndex(message, "\n") != len(message)-1 {
		message += "\n"
	}

	return SendAll(conn, []byte(message), len(message)) == len(message)
}

// Returns true if the next message from connection is an OK
func GetOKMessage(server net.Conn) bool {
	buffer := make([]byte, MIN_BUFFER_SIZE)
	n, err := server.Read(buffer)
	if err != nil {
		return false
	}

	message := string(buffer[:n])

	if strings.Index(message, "OK") != 0 {
		log.Warn(message)
		return false
	}

	return true
}

// Returns true if the next message is as expected
func CheckMessage(server net.Conn, expected string) bool {
	buffer := make([]byte, MIN_BUFFER_SIZE)
	n, err := server.Read(buffer)
	if err != nil {
		return false
	}

	return string(buffer[:n-1]) == expected
}

// Returns true if all bytes are uploaded to network
func SendAll(conn net.Conn, buffer []byte, count int) int {
	sent := 0
	n := -1

	for sent < count && n != 0 {
		n, err := conn.Write(buffer[sent:count])
		if err != nil {
			log.Warn(err)
			return -1
		}
		sent += n
	}

	return sent
}

// Remove element at index i from slice and return new slice
func RemoveIndex(arr []int, i int) []int {
	if i < 0 || i >= len(arr) {
		return arr
	}

	n := len(arr)
	arr[i], arr[n-1] = arr[n-1], arr[i]
	return arr[:n-1]
}

// Writes all bytes of given file and returns true if successful
func WriteFile(directory string, filename string, buffer []byte, blockSize int) bool {
	filepath := fmt.Sprintf("%s/%s", directory, filename)
	file, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Warn(err)
		return false
	}
	defer file.Close()

	_, err = file.Write(buffer[:blockSize])
	if err != nil {
		log.Warn(err)
		return false
	}

	return true
}

// Returns all bytes of given file or nil
func ReadFile(directory string, filename string) []byte {
	filepath := fmt.Sprintf("%s/%s", directory, filename)
	file, err := os.Open(filepath)
	if err != nil {
		log.Warn(err)
		return nil
	}

	buffer, err := io.ReadAll(file)
	if err != nil {
		log.Warn(err)
		return nil
	}

	return buffer
}

// Return list of file entries in directory which include name and size of file
func GetFilesInDirectory(directoryPath string) ([]FileEntry, error) {
	var files []FileEntry

	dir, err := os.Open(directoryPath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	dirEntries, err := dir.Readdir(0)
	if err != nil {
		return nil, err
	}

	for _, entry := range dirEntries {
		if !entry.IsDir() {
			files = append(files, FileEntry{entry.Name(), entry.Size()})
		}
	}

	return files, nil
}

// Return number of immediate files in directory
func GetFileCountInDirectory(directory string) int {
	c := 0

	dir, err := os.Open(directory)
	if err != nil {
		return 0
	}

	defer dir.Close()

	dirEntries, err := dir.Readdir(0)
	if err != nil {
		return 0
	}

	for _, entry := range dirEntries {
		if !entry.IsDir() {
			c++
		}
	}

	return c
}

// Returns true if given file exists
func FileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

// Remove element if exists and return new slice
func RemoveElement(array []string, target string) []string {
	for i, element := range array {
		if element == target {
			n := len(array)
			array[i], array[n-1] = array[n-1], array[i]
			return array[:n-1]
		}
	}

	return array
}

// Add element if not exists and return new slice
func AddUniqueElement(array []string, target string) []string {
	for _, element := range array {
		if element == target {
			return array
		}
	}

	return append(array, target)
}

func HasElement(array []string, target string) bool {
	for _, element := range array {
		if element == target {
			return true
		}
	}
	return false
}

func CloseAll(connections []net.Conn) {
	for _, conn := range connections {
		conn.Close()
	}
}

func MakeSet(values []string) map[string]bool {
	res := make(map[string]bool)
	for _, value := range values {
		res[value] = true
	}
	return res
}
