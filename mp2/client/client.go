package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	udpClient, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:6000"))

	if err != nil {
		fmt.Println("Error creating UDP client:", err)
		return
	}
	defer udpClient.Close()

	// Create a new scanner to read from stdin
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Enter text (press Ctrl+D on Unix/Linux:")

	// Read input line by line
	for scanner.Scan() {
		// Get the input text from the scanner
		text := scanner.Text()

		// Print the input text
		fmt.Println("You entered:", text)

		// testMessage := "8080:127.0.0.1:Client1"
		_, err = udpClient.Write([]byte(text))
		if err != nil {
			fmt.Println("Error sending UDP message:", err)
			return
		}

		// Receive and print the reply from the server
		replyBuffer := make([]byte, 1024)
		n, err := udpClient.Read(replyBuffer)
		if err != nil {
			fmt.Println("Error reading UDP reply:", err)
			return
		}

		replyMessage := string(replyBuffer[:n])
		fmt.Println("Received reply from server:", replyMessage)
	}
}
