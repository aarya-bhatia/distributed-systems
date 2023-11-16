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

	if info.ID == 1 {
		server := maplejuice.NewLeader(info)
		go failuredetector.NewServer(info.Hostname, info.UDPPort, common.GOSSIP_PROTOCOL, server).Start()
		go server.Start()
	} else {
		go failuredetector.NewServer(info.Hostname, info.UDPPort, common.GOSSIP_PROTOCOL, nil).Start()
		go maplejuice.StartRPCServer(info.Hostname, info.RPCPort)
	}

	<-make(chan bool)
}
