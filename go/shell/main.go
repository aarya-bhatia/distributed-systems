package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var hostnames = []string{
	"localhost",
	"fa23-cs425-0701.cs.illinois.edu",
	"fa23-cs425-0702.cs.illinois.edu",
	"fa23-cs425-0703.cs.illinois.edu",
	"fa23-cs425-0704.cs.illinois.edu",
	"fa23-cs425-0705.cs.illinois.edu",
	"fa23-cs425-0706.cs.illinois.edu",
	"fa23-cs425-0707.cs.illinois.edu",
	"fa23-cs425-0708.cs.illinois.edu",
	"fa23-cs425-0709.cs.illinois.edu",
	"fa23-cs425-0710.cs.illinois.edu",
}

const port = 3000

const SUCCESS = 0 // Task successful
const FAILURE = 1 // Task failed

// Struct of every host, including metadata for each query
type Host struct {
	host        string
	latency     string
	latencyNano int64
	dataSize    int
	lines       int
	status      int
}

// Struct for stats of each client command
type ClientStat struct {
	totalLines         uint
	averageLatency     float64
	totalBytesReceived uint
}

// Struct for clients routine
type Client struct {
	hosts    []*Host
	command  string
	stat     ClientStat
	finished chan bool
	messages chan string
}

// Creates and initializes a new Host
func NewHost(host string) *Host {
	obj := new(Host)
	obj.host = host
	obj.dataSize = 0
	obj.lines = 0
	obj.status = FAILURE

	return obj
}

// Initializes a client object, runs the query command and returns the results
func RunClient(command string) *Client {
	client := &Client{}
	client.command = command
	client.hosts = readHosts()
	client.finished = make(chan bool)
	client.messages = make(chan string)

	client.stat.totalLines = 0
	client.stat.totalBytesReceived = 0
	client.stat.averageLatency = 0

	// Create a new thread for each connection
	for _, host := range client.hosts {
		go Worker(host, client)
	}

	c := 0

	for c < len(client.hosts) {
		select {
		case message := <-client.messages:
			fmt.Print(message)
			client.stat.totalLines += 1
			client.stat.totalBytesReceived += uint(len(message))
		case <-client.finished:
			c++
		}
	}

	return client
}

func readHosts() []*Host {
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	line = line[:len(line)-1]
	hosts := []*Host{}

	for _, token := range strings.Split(line, " ") {
		idx, _ := strconv.Atoi(token)
		hosts = append(hosts, NewHost(hostnames[idx]))
	}

	return hosts
}

// Routine to communicate with specific server and execute client command
func Worker(host *Host, client *Client) {
	defer func() {
		client.finished <- true
	}()

	addr := fmt.Sprintf("%s:%d", host.host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println("Failed to connect to", addr)
		return
	}

	_, err = conn.Write([]byte(client.command + "\n"))
	conn.(*net.TCPConn).CloseWrite()

	startTime := time.Now() // Start timer
	buffer := bufio.NewReader(conn)

	for {
		str, err := buffer.ReadString('\n')
		if err != nil {
			break
		}

		if str != "\n" { // Last message is a blank line
			host.dataSize += len(str)
			host.lines++
			client.messages <- fmt.Sprintf("%s: %s", host.host, str)
		}
	}

	elapsed := time.Now().Sub(startTime) // End timer
	host.latency = elapsed.String()
	host.latencyNano = elapsed.Nanoseconds()
	host.status = SUCCESS
}

func main() {
	command := strings.Join(os.Args[1:], " ")
	client := RunClient(command)

	var count uint = 0

	for _, host := range client.hosts {
		// hostSignature := fmt.Sprintf("%s %s:%s", host.id, host.host, host.port)
		// fmt.Printf("%s, lines: %d, data: %d bytes, latency: %s (%v nanoseconds) \n", hostSignature, host.lines, host.dataSize, host.latency, host.latencyNano)
		if host.status == SUCCESS {
			count += 1
		}
	}

	fmt.Printf("Received %d lines of reply from %d hosts out of %d.\n", client.stat.totalLines, count, len(client.hosts))
}
