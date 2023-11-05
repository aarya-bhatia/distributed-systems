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

var hostnames = []string{
	"", // An empty string is used so that node IDs match their indices in list
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

const port = common.DEFAULT_FRONTEND_PORT

func runTask(task string, output chan string, done chan bool) {
	log.Println("Starting task:", task)

	defer func() {
		done <- true
	}()

	tokens := strings.Split(task, " ")
	addr := tokens[0]

	if strings.Index(addr, ":") == -1 {
		index, err := strconv.Atoi(addr)
		if err != nil {
			log.Println(err)
			return
		}
		addr = hostnames[index] + ":" + fmt.Sprint(port)
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Println(err)
		return
	}

	defer conn.Close()

	if !common.SendMessage(conn, strings.Join(tokens[1:], " ")) {
		return
	}

	scanner := bufio.NewReader(conn)

	for {
		reply, err := scanner.ReadString('\n')
		if err != nil {
			break // EOF
		}
		output <- reply[:len(reply)-1]
	}
}

// to synchronise messages on stdout
func consumer(output chan string) {
	for {
		message := <-output
		if message == "END" {
			break
		}
		fmt.Println(message)
	}
}

func startTasks(output chan string, done chan bool) int {
	numTasks := 0
	scanner := bufio.NewReader(os.Stdin)

	for {
		line, err := scanner.ReadString('\n')
		if err != nil {
			break // EOF
		}

		line = line[:len(line)-1]

		if len(line) == 0 || line[0] == '#' {
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

	return numTasks
}

func main() {
	output := make(chan string)
	done := make(chan bool)
	startTime := time.Now().UnixNano()

	go consumer(output)

	numTasks := startTasks(output, done)

	for i := 0; i < numTasks; i++ {
		<-done
	}

	output <- "END"

	endTime := time.Now().UnixNano()
	elapsedTime := float64(endTime-startTime) * 1e-9
	fmt.Printf("All tasks were finished in %.2f sec\n", elapsedTime)
}
