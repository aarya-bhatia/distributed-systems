package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func main() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
		os.Exit(1)
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("Connetion established")
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	fmt.Printf("Serving %s\n", conn.RemoteAddr().String())
	for {
		// Read incomming net message
		netData, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			break
		}

		temp := strings.TrimSpace(string(netData))
		if temp == "EOF" {
			fmt.Println("Client closes connection")
			break
		}
		fmt.Println(temp)
		args := strings.Split(temp, " ")
		// Removing quotes
		for i := 1; i < len(args); i++ {
			strconv.Unquote(args[i])
		}
		// Execute the command received
		cmd := exec.Command(args[0], args[1:]...)
		stdout, err := cmd.Output()
		if err != nil {
			fmt.Println(err.Error())
			conn.Write([]byte(err.Error()))
		} else {
			conn.Write(stdout)
		}
	}
	fmt.Printf("Closing %s\n", conn.RemoteAddr().String())
	conn.Close()
}
