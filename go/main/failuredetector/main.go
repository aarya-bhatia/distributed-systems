package main

import (
	"cs425/common"
	"cs425/failuredetector"
	"os"
	"strconv"

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

	failuredetector.NewServer(hostname, port, common.GOSSIP_PROTOCOL, nil).Start()
}
