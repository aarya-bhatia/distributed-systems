package main

import (
	"bufio"
	"cs425/common"
	"cs425/failuredetector"
	"cs425/filesystem"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

var Log = common.Log

func main() {
	var err error
	var hostname string
	var udpPort, tcpPort int
	var dbDirectory string

	systemHostname, err := os.Hostname()
	if err != nil {
		Log.Fatal(err)
	}

	flag.IntVar(&udpPort, "udp", common.DEFAULT_UDP_PORT, "failure detector port")
	flag.IntVar(&tcpPort, "tcp", common.DEFAULT_TCP_PORT, "file server port")
	flag.StringVar(&hostname, "h", systemHostname, "hostname")
	flag.StringVar(&dbDirectory, "db", "db", "database directory")
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
		Log.Fatal("Unknown Server")
	}

	if exec.Command("rm", "-rf", dbDirectory).Run() != nil {
		Log.Fatal("rm failed")
	}

	if exec.Command("mkdir", "-p", dbDirectory).Run() != nil {
		Log.Fatal("mkdir failed")
	}

	Log.Info("Directory: ", dbDirectory)

	fileServer := filesystem.NewServer(*found, dbDirectory)

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
			handleCommand(fileServer, command)
		} else {
			failureDetectorServer.InputChannel <- command
		}
	}

	// run server forever
	<-make(chan bool)
}

func handleCommand(server *filesystem.Server, command string) {
	if command == "help" {
		fmt.Println("ls: Display metadata table")
		fmt.Println("info: Display server info")
		fmt.Println("files: Display list of files")
	} else if command == "ls" {
		server.PrintFileMetadata()
	} else if command == "files" {
		for _, f := range server.Files {
			fmt.Printf("File:%s, version:%d, size:%d, numBlocks:%d\n", f.Filename, f.Version, f.FileSize, f.NumBlocks)
		}

		fmt.Println("storage:")
		files, err := common.GetFilesInDirectory(server.Directory)
		if err != nil {
			Log.Warn(err)
			return
		}
		for _, f := range files {
			tokens := strings.Split(f.Name, ":")
			fmt.Printf("File %s, version %s, block %s, size %d\n", tokens[0], tokens[1], tokens[2], f.Size)
		}
	} else if command == "info" {
		fmt.Printf("Hostname: %s, Port: %d\n", server.Hostname, server.Port)
		server.Mutex.Lock()

		fmt.Printf("Server ID: %s\nTotal files: %d\nNum alive nodes: %d\nNum disk blocks: %d\n",
			server.ID,
			len(server.Files),
			len(server.Nodes),
			common.GetFileCountInDirectory(server.Directory))

		server.Mutex.Unlock()
	}
}
