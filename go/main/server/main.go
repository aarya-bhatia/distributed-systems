package main

import (
	"bufio"
	"cs425/common"
	"cs425/failuredetector"
	"cs425/filesystem/server"
	"cs425/maplejuice"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"

	log "github.com/sirupsen/logrus"
)

func setupDB(dbDirectory string) {
	if exec.Command("rm", "-rf", dbDirectory).Run() != nil {
		log.Fatal("rm failed")
	}

	if exec.Command("mkdir", "-p", dbDirectory).Run() != nil {
		log.Fatal("mkdir failed")
	}

}

// Usage: go run . [ID]
func main() {
	common.Setup()

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	var info common.Node

	if len(os.Args) < 2 {
		info = *common.GetCurrentNode()
	} else {
		id, err := strconv.Atoi(os.Args[1])
		if err != nil {
			log.Fatal(err)
		}
		info = *common.GetNodeByID(id)
	}

	dbDirectory := path.Join(path.Join(cwd, "db"), fmt.Sprint(info.ID))
	setupDB(dbDirectory)

	log.Info("Data directory:", dbDirectory)
	log.Debug("Node:", info)

	sdfs := server.NewServer(info, dbDirectory)

	notifiers := []common.Notifier{sdfs}

	var mjLeader *maplejuice.Leader = nil
	var mjWorker *maplejuice.Service = nil

	if info.ID == common.MAPLE_JUICE_LEADER_ID {
		mjLeader = maplejuice.NewLeader(info)
		go mjLeader.Start()
		notifiers = append(notifiers, mjLeader)
	} else {
		mjWorker = maplejuice.NewService(info)
		go mjWorker.Start()
	}

	go sdfs.Start()

	fd := failuredetector.NewServer(info.Hostname, info.UDPPort, common.GOSPSIP_SUSPICION_PROTOCOL, notifiers)
	go fd.Start()

	go stdinListener(info, sdfs, fd, mjLeader, mjWorker)

	<-make(chan bool) // blocks
}

func stdinListener(info common.Node, fs *server.Server, fd *failuredetector.Server, mjLeader *maplejuice.Leader, mjWorker *maplejuice.Service) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())

		switch strings.ToLower(command) {

		case "ls":
			fallthrough

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
			fs.PrintFiles()

		case "queue":
			fs.Mutex.Lock()
			for filename, q := range fs.ResourceManager.FileQueues {
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
				continue
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Filename", "Block", "Size"})

			for _, f := range files {
				decodedFilename := common.DecodeFilename(f.Name)
				tokens := strings.Split(decodedFilename, ":")
				t.AppendRow(table.Row{
					tokens[0],
					tokens[1],
					f.Size,
				})
			}

			t.AppendSeparator()
			t.Render()

		case "jobs":
			if mjLeader == nil || info.ID != common.MAPLE_JUICE_LEADER_ID {
				continue
			}

			mjLeader.Mutex.Lock()
			fmt.Println("NumTasks:", mjLeader.NumTasks)
			for _, job := range mjLeader.Jobs {
				fmt.Println(job.Name())
			}
			for id, worker := range mjLeader.Workers {
				fmt.Printf("worker %d: %d executors, %d tasks\n", id, worker.NumExecutors, len(worker.Tasks))
			}
			mjLeader.Mutex.Unlock()

		case "stat":
			if mjLeader == nil || info.ID != common.MAPLE_JUICE_LEADER_ID {
				continue
			}

			mjLeader.Mutex.Lock()
			for _, s := range mjLeader.JobStats {
				fmt.Printf("%s: duration=%fs status=%s num_workers=%d num_tasks=%d\n",
					s.Job.Name(),
					float64(s.EndTime-s.StartTime)*1e-9,
					strconv.FormatBool(s.Status),
					s.NumWorkers,
					s.NumTasks,
				)
			}
			mjLeader.Mutex.Unlock()

		case "save":
			f, err := os.OpenFile("job_stat.csv", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
			if err != nil {
				log.Println(err)
				continue
			}
			mjLeader.Mutex.Lock()
			for _, s := range mjLeader.JobStats {
				line := fmt.Sprintf("%s,%f,%s\n", s.Job.Name(), float64(s.EndTime-s.StartTime)*1e-9, strconv.FormatBool(s.Status))
				f.Write([]byte(line))
			}
			mjLeader.Mutex.Unlock()
			f.Close()

		case "clear":
			mjLeader.Mutex.Lock()
			mjLeader.JobStats = make([]*maplejuice.JobStat, 0)
			mjLeader.Mutex.Unlock()

		case "info":
			fmt.Println("----------------------------------------------------------")
			fmt.Printf("Node: %v\n", info)
			fmt.Println("Member ID:", fd.Self.ID)
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
			// fmt.Println("queue: Print file queues status")
			fmt.Println("jobs: list maplejuice jobs")
			fmt.Println("stat: list maplejuice job stats")
			fmt.Println("clear: clear maplejuice job stats")
			fmt.Println("save: save maplejuice job stats to file")
		}
	}
}
