package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

var hosts = []string{
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
const client_exe = "/home/aaryab2/client"

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
	fmt.Printf("Process at %s finished in %.4f seconds: %s\n", host, elapsedTime.Seconds(), command)
	wg.Done()
}

func main() {
	var wg sync.WaitGroup

	if len(os.Args) < 2 {
		log.Fatal("Usage: ./multiread filename 1 2 3 ...")
	}

	input_file := os.Args[1]
	hostIds := os.Args[2:]
	output_file := input_file + ".out"

	readCommand := fmt.Sprintf("%s get %s %s", client_exe, input_file, output_file)

	for _, hostId := range hostIds {
		num, _ := strconv.Atoi(hostId)
		index := num - 1
		if index < 0 || index >= len(hosts) {
			log.Fatal("Invalid ID")
		}

		wg.Add(1)
		go run(hosts[index], readCommand, &wg)
	}

	wg.Wait()
	fmt.Printf("All subprocesses have finished.\n")
}
