package main

import (
	"bufio"
	"cs425/common"
	"cs425/failuredetector"
	"cs425/filesystem/server"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

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
		common.Cluster = common.SDFSLocalCluster
		dbDirectory = path.Join(path.Join(cwd, "db"), os.Args[1])
		info = common.Cluster[id-1]
	} else {
		common.Cluster = common.SDFSProdCluster
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

	master := server.NewServer(info, dbDirectory)
	fs := &server.FileServer{Directory: dbDirectory, Hostname: info.Hostname, TCPPort: info.TCPPort}
	fd := failuredetector.NewServer(info.Hostname, info.UDPPort, common.GOSSIP_PROTOCOL, master)

	go master.Start()
	go fd.Start()
	go fs.Start()

	go stdinListener(info, master, fd)

	<-make(chan bool) // blocks
}

func stdinListener(info common.Node, fs *server.Server, fd *failuredetector.Server) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())

		switch strings.ToLower(command) {

		case "list_mem":
			fd.PrintMembershipTable()

		case "list_self":
			fmt.Println(fd.Self.ID)

		case "kill":
			log.Fatalf("Kill command received at %d milliseconds", time.Now().UnixMilli())

		case "start_gossip":
			fallthrough

		case "join":
			fd.StartGossip()
			fmt.Println("OK")

		case "stop_gossip":
			fallthrough

		case "leave":
			fd.StopGossip()
			fmt.Println("OK")

		case "files":
			fs.PrintFileMetadata()

		case "queue":
			fs.Mutex.Lock()
			for filename, q := range fs.FileQueues {
				fmt.Printf("File %s: %d read tasks, %d write tasks, %d count, %d mode\n",
					filename,
					len(q.Reads),
					len(q.Writes),
					q.Count,
					q.Mode,
				)
			}
			fs.Mutex.Unlock()

		case "leader":
			fmt.Println(fs.GetLeaderNode())

		case "store":
			files, err := common.GetFilesInDirectory(fs.Directory)
			if err != nil {
				log.Warn(err)
				return
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Filename", "Version", "Block", "Size"})

			for _, f := range files {
				tokens := strings.Split(f.Name, ":")
				t.AppendRow(table.Row{
					tokens[0],
					tokens[1],
					tokens[2],
					f.Size,
				})
			}

			t.AppendSeparator()
			t.SetStyle(table.StyleLight)
			t.Render()

		case "info":
			fmt.Println("----------------------------------------------------------")
			fmt.Printf("Node Address: %s\n", info.Hostname)
			fmt.Printf("Ports: UDP %d, RPC %d, TCP %d\n", info.UDPPort, info.RPCPort, info.TCPPort)
			fmt.Println("Member ID:", fd.Self.ID)
			fmt.Println("Node ID:", fs.ID)
			fmt.Println("Total disk blocks", common.GetFileCountInDirectory(fs.Directory))
			fmt.Println("----------------------------------------------------------")

		case "help":
			fmt.Println("kill: crash server")
			fmt.Println("list_mem: print FD membership table")
			fmt.Println("list_self: print FD member id")
			fmt.Println("join: start gossiping")
			fmt.Println("leave: stop gossiping")
			// fmt.Println("sus_on: enable suspicion protocol")
			// fmt.Println("sus_off: disable suspicion protocol")
			fmt.Println("info: Display node info")
			fmt.Println("store: Display local files blocks")
			fmt.Println("leader: Print leader node")
			fmt.Println("files: Print file metadata")
			fmt.Println("queue: Print file queues status")
		}
	}
}
