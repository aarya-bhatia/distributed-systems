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

type Host struct {
	id       string
	host     string
	port     string
	latency  string
	dataSize int
	lines    int
}

type Queue[T any] struct {
	data    []T
	m       sync.Mutex
	cv      sync.Cond
	waiting int
}

func NewHost(id string, host string, port string) *Host {
	obj := new(Host)
	obj.id = id
	obj.host = host
	obj.port = port
	obj.latency = ""
	obj.dataSize = 0
	obj.lines = 0

	return obj
}

func (q *Queue[T]) init() {
	q.cv = *sync.NewCond(&q.m)
}

func (q *Queue[T]) push(value T) {
	q.m.Lock()
	q.data = append(q.data, value)
	q.waiting = 0
	q.cv.Signal()
	q.m.Unlock()
}

func (q *Queue[T]) pop() T {
	q.m.Lock()
	q.waiting += 1
	for len(q.data) == 0 {
		q.cv.Wait()
	}
	value := q.data[0]
	q.data = q.data[1:]
	q.waiting -= 1
	if q.waiting > 0 {
		q.cv.Signal()
	}
	q.m.Unlock()
	return value
}

func (q *Queue[T]) empty() bool {
	q.m.Lock()
	if len(q.data) == 0 {
		q.m.Unlock()
		return true
	}
	q.m.Unlock()
	return false
}

func main() {
	if len(os.Args) <= 1 {
		log.Fatal("Usage: ./client options query\n")
	}

	var silent = strings.ToLower(os.Getenv("silent")) == "true"

	hosts := readHosts()
	finishedChannel := make(chan bool)
	argsWithoutProg := strings.Join(os.Args[1:], " ")
	grepWithoutFile := fmt.Sprintf("grep %s", argsWithoutProg)

	var queue *Queue[string] = &Queue[string]{}
	queue.init()

	totalLines := 0

	// Create a new thread for each connection
	for _, host := range hosts {
		go connect(host, queue, finishedChannel, grepWithoutFile)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// Thread to receive log messages from the workers and print to stdout
	go func() {
		defer wg.Done()
		for true {
			// fmt.Println("Queue is waiting")
			value := queue.pop()
			if value == "EXIT" {
				return
			}

			if !silent {
				fmt.Print(value)
			}

			totalLines += 1
		}
	}()

	// Routine that check if all communication are finished
	go func() {
		count := 0

		for count < len(hosts) {
			<-finishedChannel
			count += 1
		}

		queue.push("EXIT")
	}()

	wg.Wait()

	fmt.Println("---Meta data---")

	for _, host := range hosts {
		hostSignature := fmt.Sprintf("%s %s:%s", host.id, host.host, host.port)
		fmt.Printf("%s, lines: %d, data: %d bytes, latency: %s\n", hostSignature, host.lines, host.dataSize, host.latency)
	}

	fmt.Printf("Total of %d lines received from server\n", totalLines)

}

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

func connect(host *Host, queue *Queue[string], finishedChannel chan bool, grepWithoutFile string) {
	conn, err := net.Dial("tcp", host.host+":"+host.port)

	if err != nil {
		log.Println(err)
		finishedChannel <- true
		return
	}

	serverSignature := fmt.Sprintf("%s %s:%s", host.id, host.host, host.port)
	log.Println("Connected to: " + serverSignature)

	var logFile = fmt.Sprintf("data/vm%s.log", host.id)
	var cmd = fmt.Sprintf("%s %s\n", grepWithoutFile, logFile)
	log.Print("Command: ", cmd)

	_, err = conn.Write([]byte(cmd))
	conn.(*net.TCPConn).CloseWrite()
	startTime := time.Now()
	connbuf := bufio.NewReader(conn)
	dataTransferred := 0
	lineCount := 0
	for {
		str, err := connbuf.ReadString('\n')

		if err != nil {
			host.latency = time.Now().Sub(startTime).String()
			host.dataSize = dataTransferred
			host.lines = lineCount
			finishedChannel <- true
			break
		}

		dataTransferred += len(str)

		if str != "\n" {
			lineCount++
			str = host.id + " " + host.host + ":" + host.port + " " + str
			queue.push(str)
		}
	}
}
