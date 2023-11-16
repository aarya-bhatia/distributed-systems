package common

import (
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

type FileEntry struct {
	Name string
	Size int64
}

type Notifier interface {
	HandleNodeJoin(node *Node)
	HandleNodeLeave(node *Node)
}

func GetNodeByID(ID int) *Node {
	for _, node := range Cluster {
		if node.ID == ID {
			return &node
		}
	}
	return nil
}

func Connect(nodeID int) (*rpc.Client, error) {
	node := GetNodeByID(nodeID)
	if node == nil {
		return nil, errors.New("Unknown Node")
	}
	addr := GetAddress(node.Hostname, node.RPCPort)
	return rpc.Dial("tcp", addr)
}

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

// Returns block name using the convention filename:blockNumber
func GetBlockName(filename string, blockNum int) string {
	return fmt.Sprintf("%s:%d", filename, blockNum)
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
	return CheckMessage(server, "OK")
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
func RemoveIndex[T comparable](arr []T, i int) []T {
	if i < 0 || i >= len(arr) {
		return arr
	}

	n := len(arr)
	arr[i], arr[n-1] = arr[n-1], arr[i]
	return arr[:n-1]
}

// Writes all bytes of given file and returns true if successful
func WriteFile(filename string, flags int, buffer []byte, blockSize int) error {
	// log.Println("Flags:",flags)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|flags, 0666)
	if err != nil {
		log.Warn(err)
		return err
	}
	defer file.Close()

	_, err = file.Write(buffer[:blockSize])
	if err != nil {
		log.Warn(err)
		return err
	}

	return nil
}

// Returns all bytes of given file or nil
func ReadFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Warn(err)
		return nil, err
	}

	buffer, err := io.ReadAll(file)
	if err != nil {
		log.Warn(err)
		return nil, err
	}

	return buffer, nil
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
func RemoveElement[T comparable](array []T, target T) []T {
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
func AddUniqueElement[T comparable](array []T, target T) []T {
	for _, element := range array {
		if element == target {
			return array
		}
	}

	return append(array, target)
}

func HasElement[T comparable](array []T, target T) bool {
	for _, element := range array {
		if element == target {
			return true
		}
	}
	return false
}

func ConnectAll(nodes []string) []net.Conn {
	connections := []net.Conn{}

	for _, addr := range nodes {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			continue
		}
		connections = append(connections, conn)
	}

	return connections
}

func CloseAll(connections []net.Conn) {
	for _, conn := range connections {
		conn.Close()
	}
}

func MakeSet[T comparable](values []T) map[T]bool {
	res := make(map[T]bool)
	for _, value := range values {
		res[value] = true
	}
	return res
}

func GetAddress(hostname string, port int) string {
	return fmt.Sprintf("%s:%d", hostname, port)
}

func EncodeFilename(name string) string {
	return strings.ReplaceAll(name, "/", "%2F")
}

func DecodeFilename(name string) string {
	return strings.ReplaceAll(name, "%2F", "/")
}

func RandomChoice[T comparable](arr []T) T {
	return arr[rand.Intn(len(arr))]
}

func Abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func Shuffle[T comparable](slice []T) []T {
	rand.Shuffle(len(slice), func(i, j int) {
		slice[i], slice[j] = slice[j], slice[i]
	})
	return slice
}

func StartRPCServer(Hostname string, Port int, Service interface{}) {
	listener, err := net.Listen("tcp4", fmt.Sprintf("%s:%d", Hostname, Port))
	if err != nil {
		log.Fatal("Error starting server: ", err)
	}

	if err := rpc.Register(Service); err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go rpc.ServeConn(conn)
	}
}
