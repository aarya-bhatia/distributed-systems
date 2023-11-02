package main

import (
	"bufio"
	"cs425/common"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func runTask(task string, output chan string, done chan bool) {
	defer func() {
		done <- true
	}()

	log.Println("Task started:", task)

	tokens := strings.Split(task, " ")
	addr := tokens[0]

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println(err)
		return
	}

	defer conn.Close()

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

func consumer(output chan string) {
	for {
		message, ok := <-output
		if !ok {
			break
		}

		fmt.Println(message)
	}
}

func main() {
	taskFile := "tasks"
	if len(os.Args) > 1 {
		taskFile = os.Args[1]
	}

	log.Println("Using file:", taskFile)

	output := make(chan string)
	go consumer(output)

	file, err := os.Open(taskFile)
	if err != nil {
		log.Fatal(err)
	}

	done := make(chan bool)
	scanner := bufio.NewScanner(file)
	startTime := time.Now().UnixNano()
	numTasks := 0

	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println(line)

		if line[0] == '#' {
			continue
		}

		if strings.Index(line, "sleep") == 0 {
			interval, err := strconv.Atoi(line[6:])
			if err != nil {
				log.Println(err)
			}
			log.Println("Sleeping for", interval, "seconds")
			time.Sleep(time.Duration(interval) * time.Second)
		} else {
			go runTask(line, output, done)
			numTasks++
		}
	}

	c := 0

	for c < numTasks {
		<-done
		c++
	}

	close(output)
	close(done)

	endTime := time.Now().UnixNano()
	elapsedTime := float64(endTime-startTime) * 1e-9

	fmt.Printf("All tasks were finished in %.2f sec\n", elapsedTime)
}
