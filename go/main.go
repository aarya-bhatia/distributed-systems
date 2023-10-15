package main

import (
	"bufio"
	"cs425/common"
	"cs425/failuredetector"
	"cs425/filesystem"
	"flag"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

func main() {
	var err error
	var udpPort, tcpPort int
	var hostname, level, env string
	var withSuspicion bool

	systemHostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	flag.IntVar(&udpPort, "udp", common.DEFAULT_UDP_PORT, "port number for failure detector")
	flag.IntVar(&tcpPort, "tcp", common.DEFAULT_TCP_PORT, "port number for file server")
	flag.StringVar(&hostname, "h", systemHostname, "server hostname")
	flag.StringVar(&level, "l", "DEBUG", "log level")
	flag.StringVar(&env, "e", "production", "environment: development, production")
	flag.BoolVar(&withSuspicion, "s", false, "gossip with suspicion")
	flag.Parse()

	if hostname == "localhost" || hostname == "127.0.0.1" {
		env = "development"
	}

	if env == "development" {
		hostname = "localhost"
		log.Info("Using local cluster")
		common.Cluster = common.LocalCluster
	} else {
		log.Info("Using prod cluster")
		common.Cluster = common.ProdCluster
	}

	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})

	log.SetReportCaller(true)
	log.SetOutput(os.Stderr)

	switch strings.ToLower(level) {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	}

	log.Debug("Cluster: ", common.Cluster)

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
	go fileServer.Start()

	// defer fileServer.Close()

	protocol := common.GOSSIP_PROTOCOL

	if withSuspicion {
		protocol = common.GOSPSIP_SUSPICION_PROTOCOL
	}

	failureDetectorServer, err := failuredetector.NewServer(found.Hostname, found.UDPPort, protocol, fileServer)
	if err != nil {
		log.Fatal(err)
	}

	// defer failureDetectorServer.Close()
	// go failureDetectorServer.Start()
	// log.Infof("Node %s: failure detector running on port %d\n", failureDetectorServer.Self.ID, failureDetectorServer.Self.Port)

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
			fileServer.HandleCommand(command)
		} else {
			failureDetectorServer.InputChannel <- command
		}
	}
}
