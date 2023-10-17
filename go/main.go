package main

import (
	"bufio"
	"cs425/common"
	"cs425/failuredetector"
	"cs425/filesystem"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

var Log = common.Log

func main() {
	var err error
	var hostname string
	var udpPort, tcpPort int

	systemHostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	flag.IntVar(&udpPort, "udp", common.DEFAULT_UDP_PORT, "failure detector port")
	flag.IntVar(&tcpPort, "tcp", common.DEFAULT_TCP_PORT, "file server port")
	flag.StringVar(&hostname, "h", systemHostname, "hostname")
	flag.Parse()

	if hostname == "localhost" || hostname == "127.0.0.1" {
		Log.Info("Using local cluster")
		common.Cluster = common.LocalCluster
	} else {
		Log.Info("Using prod cluster")
		common.Cluster = common.ProdCluster
	}

	Log.Debug(common.Cluster)

	var found *common.Node = nil

	for _, node := range common.Cluster {
		if node.Hostname == hostname && node.UDPPort == udpPort && node.TCPPort == tcpPort {
			found = &node
			break
		}
	}

	if found == nil {
		log.Fatal("Unknown Server")
	}

	fileServer := filesystem.NewServer(*found)

	failureDetectorServer := failuredetector.NewServer(found.Hostname, found.UDPPort, common.GOSPSIP_SUSPICION_PROTOCOL, fileServer)

	go fileServer.Start()
	go failureDetectorServer.Start()

	mode := 1
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		tokens := strings.Split(command, " ")

		if tokens[0] == "help" {
			fmt.Println("mode 0: change mode to failure detector")
			fmt.Println("mode 1: change mode to file system")
		} else if tokens[0] == "mode" {
			if tokens[1] == "1" {
				fmt.Println("Set mode to file system")
				mode = 1
			} else {
				fmt.Println("Set mode to failure detector")
				mode = 0
			}
		}

		if mode == 1 {
			fileServer.InputChannel <- command
		} else {
			failureDetectorServer.InputChannel <- command
		}
	}
}
