package main

import (
	"cs425/common"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

func runTask(task string, output chan string, done chan bool) {
	defer func() {
		done <- true
	}()

	tokens := strings.Split(task, " ")
	addr := tokens[0]

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println(err)
		return
	}

	request := strings.Join(tokens[1:], " ")
	log.Println(addr, request)
	if !common.SendMessage(conn, request) {
		return
	}

	buffer := make([]byte, common.MIN_BUFFER_SIZE)
	n, err := conn.Read(buffer)
	if err != nil {
		log.Println(err)
		return
	}

	output <- string(buffer[:n-1])
}

func main() {
	taskFile := "tasks"
	if len(os.Args) > 1 {
		taskFile = os.Args[1]
	}

	file, _ := os.Open(taskFile)
	data, _ := io.ReadAll(file)
	lines := strings.Split(string(data), "\n")

	numTasks := 0
	output := make(chan string)
	done := make(chan bool)

	startTime := time.Now().UnixNano()

	for _, line := range lines {
		if len(line) <= 1 || line[0] == '#' {
			continue
		}

		line = line[:len(line)-1]
		go runTask(line, output, done)
	}

	c := 0

	for c < numTasks {
		select {
		case message := <-output:
			fmt.Println(message)
		case <-done:
			c++
		}
	}

	endTime := time.Now().UnixNano()
	elapsedTime := float64(endTime-startTime) * 1e-9

	fmt.Printf("All tasks were finished in %.2f sec\n", elapsedTime)
}
