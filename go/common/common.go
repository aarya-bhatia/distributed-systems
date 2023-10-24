package common

import (
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

const (
	MAX_NODES       = 10
	BLOCK_SIZE      = 1024 * 1024
	MIN_BUFFER_SIZE = 1024
	REPLICA_FACTOR  = 4

	POLL_INTERVAL      = 1 * time.Second
	REBALANCE_INTERVAL = 2 * time.Second

	JOIN_RETRY_TIMEOUT = time.Second * 10

	NODES_PER_ROUND = 4 // Number of random peers to send gossip every round

	GOSSIP_PROTOCOL            = 0
	GOSPSIP_SUSPICION_PROTOCOL = 1

	NODE_ALIVE     = 0
	NODE_SUSPECTED = 1
	NODE_FAILED    = 2

	DEFAULT_TCP_PORT = 5000
	DEFAULT_UDP_PORT = 6000
)

type Node struct {
	ID       int
	Hostname string
	UDPPort  int
	TCPPort  int
}

type FileEntry struct {
	Name string
	Size int64
}

type Notifier interface {
	HandleNodeJoin(node *Node)
	HandleNodeLeave(node *Node)
}

type Logger struct {
	InfoLogger  *log.Logger
	WarnLogger  *log.Logger
	DebugLogger *log.Logger
	FatalLogger *log.Logger
}

func NewLogger() *Logger {
	l := new(Logger)
	l.InfoLogger = log.New(os.Stderr, "INFO ", log.Ldate|log.Ltime|log.Lshortfile)
	l.WarnLogger = log.New(os.Stderr, "WARN ", log.Ldate|log.Ltime|log.Lshortfile)
	l.DebugLogger = log.New(os.Stderr, "DEBUG ", log.Ldate|log.Ltime|log.Lshortfile)
	l.FatalLogger = log.New(os.Stderr, "ERROR ", log.Ldate|log.Ltime|log.Lshortfile)

	return l
}

func (l *Logger) Fatalf(format string, args ...any) {
	l.FatalLogger.Fatalf(format, args...)
}

func (l *Logger) Infof(format string, args ...any) {
	l.InfoLogger.Printf(format, args...)
}

func (l *Logger) Warnf(format string, args ...any) {
	l.WarnLogger.Printf(format, args...)
}

func (l *Logger) Debugf(format string, args ...any) {
	l.DebugLogger.Printf(format, args...)
}

func (l *Logger) Fatal(args ...any) {
	l.FatalLogger.Fatal(args...)
}

func (l *Logger) Info(args ...any) {
	l.InfoLogger.Println(args...)
}

func (l *Logger) Warn(args ...any) {
	l.WarnLogger.Println(args...)
}

func (l *Logger) Debug(args ...any) {
	l.DebugLogger.Println(args...)
}

var Log = NewLogger()

var ProdCluster = []Node{
	{1, "fa23-cs425-0701.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{2, "fa23-cs425-0702.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{3, "fa23-cs425-0703.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{4, "fa23-cs425-0704.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{5, "fa23-cs425-0705.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{6, "fa23-cs425-0706.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{7, "fa23-cs425-0707.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{8, "fa23-cs425-0708.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{9, "fa23-cs425-0709.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
	{10, "fa23-cs425-0710.cs.illinois.edu", DEFAULT_UDP_PORT, DEFAULT_TCP_PORT},
}

var LocalCluster = []Node{
	{1, "localhost", 6001, 5001},
	{2, "localhost", 6002, 5002},
	{3, "localhost", 6003, 5003},
	{4, "localhost", 6004, 5004},
	{5, "localhost", 6005, 5005},
}

var Cluster []Node = LocalCluster

func GetNode(id int) Node {
	return Cluster[id-1]
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

func GetOKMessage(server net.Conn) bool {
	buffer := make([]byte, MIN_BUFFER_SIZE)
	n, err := server.Read(buffer)
	if err != nil {
		Log.Warn(err)
		return false
	}

	Log.Debugf("Received %d bytes\n", n)
	message := string(buffer[:n])

	if strings.Index(message, "OK") != 0 {
		Log.Warn(message)
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
			Log.Warn(err)
			return -1
		}
		sent += n
	}

	Log.Debugf("Sent %d bytes to %s\n", sent, conn.RemoteAddr())

	return sent
}

func RemoveIndex(arr []int, i int) []int {
	if i < 0 || i >= len(arr) {
		return arr
	}

	n := len(arr)
	arr[i], arr[n-1] = arr[n-1], arr[i]
	return arr[:n-1]
}

func WriteFile(directory string, filename string, buffer []byte, blockSize int) bool {
	filepath := fmt.Sprintf("%s/%s", directory, filename)
	file, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		Log.Warn(err)
		return false
	}
	defer file.Close()

	_, err = file.Write(buffer[:blockSize])
	if err != nil {
		Log.Warn(err)
		return false
	}

	return true
}

func ReadFile(directory string, filename string) []byte {
	filepath := fmt.Sprintf("%s/%s", directory, filename)
	file, err := os.Open(filepath)
	if err != nil {
		Log.Warn(err)
		return nil
	}

	buffer, err := io.ReadAll(file)
	if err != nil {
		Log.Warn(err)
		return nil
	}

	return buffer
}

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
