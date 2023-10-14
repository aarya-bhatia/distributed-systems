package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func StartTCPServer(tcpPort int) {
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(tcpPort))

	if err != nil {
		fmt.Printf("Error starting server: %s\n", err)
		return
	}
	defer listener.Close()

	fmt.Printf("TCP Server is listening on port %d...\n", tcpPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %s\n", err)
			continue
		}

		fmt.Printf("Accepted connection from %s\n", conn.RemoteAddr())
		go handleTCPConnection(conn)
	}
}

func handleTCPConnection(conn net.Conn) {
	defer conn.Close()

	// Create a buffer to hold incoming data
	buffer := make([]byte, 1024)

	for {
		// Read data from the client
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Client %s disconnected\n", conn.RemoteAddr())
			return
		}

		request := string(buffer[:n])
		fmt.Printf("Received request from %s: %s\n", conn.RemoteAddr(), request)

		lines := strings.Split(request, "\n")
		tokens := strings.Split(lines[0], " ")
		verb := strings.ToUpper(tokens[0])

		if strings.Index(verb, "EXIT") == 0 {
			return
		}

		response := []byte("OK\n")

		// Echo the data back to the client
		_, err = conn.Write(response)
		if err != nil {
			fmt.Printf("Error writing to client: %s\n", err)
			return
		}
	}
}

