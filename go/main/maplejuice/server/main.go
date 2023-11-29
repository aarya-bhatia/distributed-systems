package main

import (
	"bufio"
	"cs425/common"
	"cs425/maplejuice"
	"fmt"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

func main() {
	common.Setup()

	var info common.Node

	if len(os.Args) > 1 {
		id, err := strconv.Atoi(os.Args[1])
		if err != nil {
			log.Fatal(err)
		}
		info = *common.GetNodeByID(id, common.MapleJuiceCluster)
	} else {
		info = *common.GetCurrentNode(common.MapleJuiceCluster)
	}

	if info.ID == common.MAPLE_JUICE_LEADER_ID {
		server := maplejuice.NewLeader(info)
		go server.Start()
		go leaderStdinHandler(info, server)
	} else {
		service := maplejuice.NewService(info)
		go service.Start()
		go workerStdinHandler(info, service)
	}

	<-make(chan bool)
}

func leaderStdinHandler(info common.Node, server *maplejuice.Leader) {
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		switch strings.ToLower(command) {
		case "info":
			fmt.Println("MapleJuice Leader node: ", info)
			fmt.Println("Nodes:", server.Nodes)
		case "jobs":
			server.Mutex.Lock()
			fmt.Println("NumTasks:", server.NumTasks)
			for _, job := range server.Jobs {
				fmt.Println(job.Name())
			}
			for id, worker := range server.Workers {
				fmt.Printf("worker %d: %d executors, %d tasks\n", id, worker.NumExecutors, len(worker.Tasks))
			}
			server.Mutex.Unlock()
		case "stat":
			server.Mutex.Lock()
			for _, s := range server.JobStats {
				fmt.Printf("%s: duration=%fs status=%s num_workers=%d num_tasks=%d\n",
					s.Job.Name(),
					float64(s.EndTime-s.StartTime)*1e-9,
					strconv.FormatBool(s.Status),
					s.NumWorkers,
					s.NumTasks,
				)
			}
			server.Mutex.Unlock()
		case "help":
			fmt.Println("jobs: list maplejuice jobs")
			fmt.Println("info: display node info")
		}
	}
}

func workerStdinHandler(info common.Node, server *maplejuice.Service) {
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		switch strings.ToLower(command) {
		case "info":
			fmt.Println("MapleJuice Worker node: ", info)
		case "data":
			server.Mutex.Lock()
			for k := range server.Data {
				fmt.Println(k)
			}
			server.Mutex.Unlock()
		case "help":
			fmt.Println("info: display node info")
		}
	}
}
