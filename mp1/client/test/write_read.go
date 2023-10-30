package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const port = 3000

func run(host string, command string, wg *sync.WaitGroup) {
	startTime := time.Now()
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Println("failed to connect to", host)
		return
	}

	log.Println("connected to", host)

	_, err = conn.Write([]byte(command))
	if err != nil {
		log.Println(err)
		return
	}

	conn.(*net.TCPConn).CloseWrite()
	buffer := bufio.NewReader(conn)
	for {
		str, err := buffer.ReadString('\n')
		if err != nil {
			break
		}
		if str != "\n" {
			log.Println(host, ":", str[:len(str)-1])
		}
	}

	elapsedTime := time.Now().Sub(startTime)
	fmt.Printf("Process at %s finished in %.2f seconds: %s\n", host, elapsedTime.Seconds(), command)
	wg.Done()
}

func main() {
	fmt.Println("hello")

	var wg sync.WaitGroup

	hosts := []string{}

	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		host := line[:len(line)-1]
		hosts = append(hosts, host)
	}

	writeCommand := fmt.Sprintf("/home/aaryab2/client put /home/aaryab2/file file")
	readCommand := fmt.Sprintf("/home/aaryab2/client get file /home/aaryab2/file.out")

	if len(hosts) < 2 {
		return
	}

	wg.Add(1)
	go run(hosts[0], writeCommand, &wg)

	for _, host := range hosts[2:] {
		wg.Add(1)
		go run(host, writeCommand, &wg)
	}

	time.Sleep(5 * time.Second)

	wg.Add(1)
	go run(hosts[1], readCommand, &wg)

	wg.Wait()
	fmt.Printf("All subprocesses have finished.\n")
}
