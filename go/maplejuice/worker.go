package maplejuice

import (
	"cs425/common"
	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
	"strings"
)

const (
	RPC_MAP_TASK   = "Service.MapTask"
	RPC_JUICE_TASK = "Service.JuiceTask"
)

// RPC handler
type Service struct {
}

// Arguments for a MapTask RPC call
type MapArgs struct {
	Task *MapTask
	Data []string
}

func StartRPCServer(Hostname string, Port int) {
	service := new(Service)

	if err := rpc.Register(service); err != nil {
		log.Fatal(err)
	}

	addr := common.GetAddress(Hostname, Port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	log.Info("MapleJuice worker is running at ", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		go rpc.ServeConn(conn)
	}
}

func WordCountMapper(lines []string) map[string]int {
	res := make(map[string]int)
	for _, line := range lines {
		for _, word := range strings.Split(line, " ") {
			res[word]++
		}
	}
	return res
}

func (service *Service) MapTask(args *MapArgs, reply *bool) error {
	log.Println("Recevied map task:", args.Task, len(args.Data), "lines")

	for key, value := range WordCountMapper(args.Data) {
		fname := args.Task.Param.OutputPrefix + "_" + common.EncodeFilename(key)
		log.Println(fname, key, value)
	}

	*reply = true
	return nil
}
