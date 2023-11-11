package maplejuice

import (
	"cs425/common"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
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

	dirname := fmt.Sprintf("output/%s", service.Address)
	if err := os.MkdirAll(dirname, 0755); err != nil {
		log.Println(err)
		*reply = false
		return nil
	}

	for key, value := range WordCountMapper(args.Data) {
		fname := dirname + "/" + args.Task.OutputPrefix + "_" + common.EncodeFilename(key)
		log.Println(fname)
		f, err := os.OpenFile(fname, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			log.Println(err)
			*reply = false
			return nil
		}
		f.WriteString(fmt.Sprintf("%d\n", value))
		f.Close()
	}

	*reply = true
	return nil
}
