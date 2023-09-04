package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

type Host struct {
	id   string
	host string
	port string
}

func main() {
	hosts := readHosts()

	outputChannel := make(chan string)
	finishedChannel := make(chan bool)

	argsWithoutProg := strings.Join(os.Args[1:], " ")
	cmd := []byte(argsWithoutProg + "\n")

	for _, host := range hosts {
		go connect(host, outputChannel, finishedChannel, cmd)
	}

	go func() {
		count := 0

		for count < len(hosts) {
			<-finishedChannel
			count += 1
		}

		close(outputChannel)
	}()

	for str := range outputChannel {
		fmt.Print(str)
	}
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

func connect(host Host, channel chan string, finishedChannel chan bool, cmd []byte) {
	conn, err := net.Dial("tcp", host.host+":"+host.port)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Connected to: " + conn.LocalAddr().String())
	_, err = conn.Write(cmd)
	conn.(*net.TCPConn).CloseWrite()
	connbuf := bufio.NewReader(conn)

	for {
		str, err := connbuf.ReadString('\n')

		if err != nil {
			finishedChannel <- true
			break
		}

		if str != "\n" {
			channel <- str
		}
	}
}
