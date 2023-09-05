package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

type Host struct {
	id   string
	host string
	port string
}

type MetaData struct {
	latency  string
	dataSize int
	lines    int
	host     Host
}

func main() {
	if len(os.Args) <= 1 {
		log.Fatal("Please enter command")
	}
	hosts := readHosts()

	outputChannel := make(chan string)
	finishedChannel := make(chan bool)
	metaDataChannel := make(chan MetaData, 4096)

	argsWithoutProg := strings.Join(os.Args[1:], " ")
	cmd := []byte(argsWithoutProg + "\n")

	for _, host := range hosts {
		go connect(host, outputChannel, finishedChannel, metaDataChannel, cmd)
	}

	// Routine that check if all communication are finished
	go func() {
		count := 0

		for count < len(hosts) {
			<-finishedChannel
			count += 1
		}

		close(outputChannel)
		close(metaDataChannel)
	}()
	totalLines := 0

	for str := range outputChannel {
		fmt.Print(str)
		totalLines++
	}
	fmt.Println("---Meta data---")
	for metaData := range metaDataChannel {
		hostSignature := fmt.Sprintf("%s %s:%s", metaData.host.id, metaData.host.host, metaData.host.port)
		fmt.Printf("%s, lines: %d, data: %d, latency: %s\n", hostSignature, metaData.lines, metaData.dataSize, metaData.latency)
	}
	fmt.Printf("Total of %d lines received from server\n", totalLines)
}

func readHosts() []Host {
	hosts := []Host{}
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
		hosts = append(hosts, Host{words[0], words[1], words[2]})
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Println(hosts)
	return hosts
}

func connect(host Host, channel chan string, finishedChannel chan bool, metaDataChannel chan MetaData, cmd []byte) {
	conn, err := net.Dial("tcp", host.host+":"+host.port)
	if err != nil {
		fmt.Println(err)
	}
	serverSignature := fmt.Sprintf("%s %s:%s", host.id, host.host, host.port)
	fmt.Println("Connected to: " + serverSignature)
	_, err = conn.Write(cmd)
	conn.(*net.TCPConn).CloseWrite()
	startTime := time.Now()
	connbuf := bufio.NewReader(conn)
	dataTransferred := 0
	lineCount := 0
	for {
		str, err := connbuf.ReadString('\n')

		if err != nil {
			metaData := MetaData{time.Now().Sub(startTime).String(), dataTransferred, lineCount, host}
			metaDataChannel <- metaData
			finishedChannel <- true
			break
		}
		dataTransferred += len(str)
		if str != "\n" {
			lineCount++
			str = host.id + " " + host.host + ":" + host.port + " " + str
			channel <- str
		}
	}
}
