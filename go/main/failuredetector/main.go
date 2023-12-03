package main

import (
	"bufio"
	"cs425/common"
	"cs425/failuredetector"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

func printUsage() {
	logrus.Println("Usage: ./failuredetector HOSTNAME PORT")
	os.Exit(1)
}

func main() {
	common.Setup()

	if len(os.Args) < 3 {
		printUsage()
	}

	hostname := os.Args[1]
	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		logrus.Fatal(err)
	}

	fd := failuredetector.NewServer(hostname, port, common.GOSPSIP_SUSPICION_PROTOCOL, nil)
	go fd.Start()
	go stdinListener(fd)

	// TODO Add command to force kill nodes

	<-make(chan bool) // blocks
}

func stdinListener(fd *failuredetector.Server) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		tokens := strings.Fields(scanner.Text())
		switch tokens[0] {
		case "ls":
			fd.PrintMembershipTable()
		}
	}
}
