package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const STATUS_SUCCESS = 0
const STATUS_FAILURE = 1

type Host struct {
	id       string
	host     string
	port     string
	latency  string
	dataSize int
	lines    int
	status   int
}

type ClientStat struct {
	totalLines         uint
	averageLatency     float64
	totalBytesReceived uint
}

type ClientArgs struct {
	command         string
	grep            string
	outputDirectory string
	logsDirectory   string
	silence         bool
}

type Client struct {
	finishedChannel chan bool
	queue           *Queue[string]
	hosts           []*Host
	wg              *sync.WaitGroup
	args            ClientArgs
	stat            ClientStat
}

const EXIT_MESSAGE = "EXIT"
const DEFAULT_FILE_MODE = 0664
const DEFAULT_DIRECTORY_MODE = 0775

// Creates and initialize a new Host
func NewHost(id string, host string, port string) *Host {
	obj := new(Host)
	obj.id = id
	obj.host = host
	obj.port = port
	obj.dataSize = 0
	obj.lines = 0
	obj.status = STATUS_FAILURE

	return obj
}

func RunClient(args ClientArgs) *Client {
	client := &Client{}
	client.args = args
	client.hosts = readHosts()
	client.finishedChannel = make(chan bool)
	client.queue = &Queue[string]{}
	client.queue.init()

	client.stat.totalLines = 0
	client.stat.totalBytesReceived = 0
	client.stat.averageLatency = 0

	client.wg = &sync.WaitGroup{}
	client.wg.Add(2)

	log.Println(client)

	go FinishedChannelRoutine(client)
	go OutputConsumerRoutine(client)

	// Create a new thread for each connection
	for _, host := range client.hosts {
		go Worker(host, client)
	}

	client.wg.Wait()

	return client
}

// Routine to check if all communications are finished
func FinishedChannelRoutine(client *Client) {
	defer client.wg.Done()

	count := 0

	for count < len(client.hosts) {
		<-client.finishedChannel
		count += 1
	}

	client.queue.push(EXIT_MESSAGE)
}

// Routine to consume the output lines received from the servers
func OutputConsumerRoutine(client *Client) {
	defer client.wg.Done()

	for true {
		message := client.queue.pop()

		if message == EXIT_MESSAGE {
			break
		}

		if !client.args.silence {
			fmt.Print(message)
		}

		client.stat.totalLines += 1
		client.stat.totalBytesReceived += uint(len(message))
	}
}

// Read the hosts file and initialize host array
func readHosts() []*Host {
	hosts := []*Host{}
	file, err := os.Open("./hosts")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		// Ignore lines that start with '!', are empty, or contain only whitespace
		if strings.HasPrefix(line, "!") || strings.TrimSpace(line) == "" {
			continue
		}

		words := strings.Split(line, " ")
		host := NewHost(words[0], words[1], words[2])
		hosts = append(hosts, host)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	log.Println(hosts)
	return hosts
}

// Routine to communicate with specific server and execute client command
// Saves the output to the file "<outputDirectory>/vm<ID>.output"
func Worker(host *Host, client *Client) {
	defer func() {
		client.finishedChannel <- true
	}()

	conn, err := net.Dial("tcp", host.host+":"+host.port)

	if err != nil {
		log.Println(err)
		return
	}

	outputFilename := fmt.Sprintf("%s/vm%s.output", client.args.outputDirectory, host.id)
	outputFile, err := os.OpenFile(outputFilename, os.O_CREATE|os.O_WRONLY, DEFAULT_FILE_MODE)

	if err != nil {
		log.Println(err)
		return
	}

	defer outputFile.Close()

	serverSignature := fmt.Sprintf("%s %s:%s", host.id, host.host, host.port)
	log.Println("Connected to: " + serverSignature)

	serverLogFile := fmt.Sprintf("%s/vm%s.log", client.args.logsDirectory, host.id)

	var command string

	if client.args.grep != "" {
		command = fmt.Sprintf("%s %s\n", client.args.grep, serverLogFile)
	} else {
		command = client.args.command + "\n"
	}

	log.Print("Command: ", command)
	_, err = conn.Write([]byte(command))
	conn.(*net.TCPConn).CloseWrite()

	startTime := time.Now() // Start timer
	buffer := bufio.NewReader(conn)

	for {
		str, err := buffer.ReadString('\n')

		if err != nil {
			host.latency = time.Now().Sub(startTime).String() // end timer
			break
		}

		if str != "\n" {
			host.dataSize += len(str)
			host.lines++

			outputStr := fmt.Sprintf("%s %s:%s %s", host.id, host.host, host.port, str)
			client.queue.push(outputStr)
			outputFile.WriteString(str)
		}
	}

	host.status = STATUS_SUCCESS
}
