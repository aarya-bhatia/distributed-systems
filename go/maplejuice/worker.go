package maplejuice

import (
	"cs425/common"
	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
)

type Service struct {
	Address string
}

func StartRPCServer(Hostname string, Port int) {
	addr := common.GetAddress(Hostname, Port)

	service := new(Service)
	service.Address = addr

	if err := rpc.Register(service); err != nil {
		log.Fatal(err)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	log.Info("MapleJuice worker is running at ", addr)

	for {
		rpc.Accept(listener)
	}
}

func (service *Service) MapTask(args *MapArgs, reply *bool) error {
	log.Println("Recevied map task:", args.Task)
	log.Println("Data:", len(args.Data), "lines")
	*reply = true
	return nil
}
