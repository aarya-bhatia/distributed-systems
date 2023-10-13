package main

import (
	"fmt"
	"net"
)

const (
	ENV            = "DEV"
	DEFAULT_PORT   = 5000
	MAX_BLOCK_SIZE = 4096 * 1024 // 4 MB
)

type Node struct {
	Hostname string
	Port     int
}

var cluster = []Node{
	{"fa23-cs425-0701.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0702.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0703.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0704.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0705.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0706.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0707.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0708.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0709.cs.illinois.edu", DEFAULT_PORT},
	{"fa23-cs425-0710.cs.illinois.edu", DEFAULT_PORT},
}

var nodes = []Node{
	{"localhost", 5000},
	{"localhost", 5001},
	{"localhost", 5002},
	{"localhost", 5003},
	{"localhost", 5004},
}

func sendMessage(message string, conn net.Conn) {
}

func clientProtocol(conn net.Conn) {
	clientMessage := "Hello world\n"

	_, err := conn.Write([]byte(clientMessage))
	if err != nil {
		fmt.Printf("Error sending data to the server: %s\n", err)
		return
	}

	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Printf("Error reading server response: %s\n", err)
		return
	}

	serverResponse := string(buffer[:n])
	fmt.Print("Server response: ", serverResponse)
	conn.Write([]byte("EXIT\n"))
}

func main() {
	for _, node := range nodes {
		addr := fmt.Sprintf("%s:%d", node.Hostname, node.Port)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("Error connecting to the server: %s\n", err)
			continue
		}
		defer conn.Close()
		clientProtocol(conn)
		break
	}
}
