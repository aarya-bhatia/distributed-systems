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

const port = common.DEFAULT_FRONTEND_PORT

func runTask(task string, output chan string, done chan bool) {
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

	log.Printf("Starting task for node %s: %s\n", addr, task)

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

	scanner := bufio.NewReader(conn)

	for {
		reply, err := scanner.ReadString('\n')
		if err != nil { // EOF
			log.Println(err)
			break
		}

		reply = reply[:len(reply)-1]

		// if reply == "UPLOAD_OK" || reply == "UPLOAD_ERROR" ||
		// 	reply == "DOWNLOAD_OK" || reply == "DOWNLOAD_ERROR" ||
		// 	reply == "DELETE_OK" || reply == "DELETE_ERROR" {
		// 	break
		// }

		output <- reply
	}

	// buffer := make([]byte, common.MIN_BUFFER_SIZE)
	// n, err := conn.Read(buffer)
	// if err != nil {
	// 	log.Println(err)
	// 	return
	// }

	// output <- string(buffer[:n-1])
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
	output := make(chan string)
	go consumer(output)

	done := make(chan bool)
	scanner := bufio.NewReader(os.Stdin)
	startTime := time.Now().UnixNano()
	numTasks := 0

	for {
		line, err := scanner.ReadString('\n')
		if err != nil {
			log.Println(err)
			break
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
