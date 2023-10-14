package main

import (
	"fmt"
	"net"
	"strconv"
)

func StartUDPServer(udpPort int) {
	udpAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(udpPort))
	if err != nil {
		fmt.Printf("Error resolving address: %s\n", err)
		return
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		fmt.Printf("Error creating UDP connection: %s\n", err)
		return
	}
	defer conn.Close()

	fmt.Printf("UDP server is listening on port %d...\n", udpPort)

	buffer := make([]byte, 1024)

	for {
		n, addr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Printf("Error reading from UDP connection: %s\n", err)
			continue
		}

		data := string(buffer[:n])
		fmt.Printf("Received data from %s: %s\n", addr, data)
	}
}
