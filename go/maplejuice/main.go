package main

import (
	"cs425/common"
	"cs425/failuredetector"
	"cs425/maplejuice/leader"
	"cs425/maplejuice/worker"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

func main() {
	common.Cluster = common.LocalMapReduceCluster // TODO

	if len(os.Args) < 2 {
		log.Fatal("Usage: maplejuice <ID>")
		return
	}

	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	info := common.LocalMapReduceCluster[id-1]

	if id == 1 {
		server := leader.NewLeader(info)
		go failuredetector.NewServer(info.Hostname, info.UDPPort, common.GOSSIP_PROTOCOL, server).Start()
		go server.Start()
	} else {
		go failuredetector.NewServer(info.Hostname, info.UDPPort, common.GOSSIP_PROTOCOL, nil).Start()
		go worker.Start(info.Hostname, info.TCPPort)
	}

	<-make(chan bool)
}
