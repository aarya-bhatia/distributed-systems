package main

import (
	"cs425/common"
	"cs425/failuredetector"
	"cs425/maplejuice"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
)

func main() {
	common.Setup()

	if len(os.Args) < 2 {
		log.Fatal("Usage: maplejuice <ID>")
		return
	}

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
		go failuredetector.NewServer(info.Hostname, info.UDPPort, common.GOSSIP_PROTOCOL, server).Start()
		go server.Start()
	} else {
		service := maplejuice.NewService(info.ID, info.Hostname, info.RPCPort)
		go failuredetector.NewServer(info.Hostname, info.UDPPort, common.GOSSIP_PROTOCOL, service).Start()
		go service.Start()
	}

	<-make(chan bool)
}
