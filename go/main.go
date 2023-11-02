package main

import (
	"bufio"
	"cs425/common"
	"cs425/failuredetector"
	"cs425/filesystem"
	"cs425/frontend"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

func findNode(hostname string) *common.Node {
	for _, node := range common.Cluster {
		if node.Hostname == hostname {
			return &node
		}
	}

	return nil
}

// Usage: go run . [ID]
func main() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)

	log.SetReportCaller(false)
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stderr)

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	var dbDirectory string
	var info common.Node

	if len(os.Args) > 1 {
		id, err := strconv.Atoi(os.Args[1])
		if err != nil {
			log.Fatal(err)
		}
		common.Cluster = common.LocalCluster
		dbDirectory = path.Join(path.Join(cwd, "db"), os.Args[1])
		info = common.Cluster[id-1]
	} else {
		common.Cluster = common.ProdCluster
		dbDirectory = path.Join(cwd, "db")
		found := findNode(hostname)
		if found == nil {
			log.Fatal("Unknown node")
		}
		info = *found
	}

	if exec.Command("rm", "-rf", dbDirectory).Run() != nil {
		log.Fatal("rm failed")
	}

	if exec.Command("mkdir", "-p", dbDirectory).Run() != nil {
		log.Fatal("mkdir failed")
	}

	log.Info("Data directory:", dbDirectory)
	log.Debug("Cluster:", common.Cluster)
	log.Debug("Node Info:", info)

	fileServer := filesystem.NewServer(info, dbDirectory)

	failureDetectorServer := failuredetector.NewServer(info.Hostname,
		info.UDPPort, common.GOSPSIP_SUSPICION_PROTOCOL, fileServer)

	frontend := frontend.NewServer(info, fileServer)

	go fileServer.Start()
	go failureDetectorServer.Start()
	go frontend.Start()

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

// TODO
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
			log.Warn(err)
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
