package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	"net"
	"net/rpc"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

const (
	RPC_MAP_TASK   = "Service.MapTask"
	RPC_JUICE_TASK = "Service.JuiceTask"
)

// RPC handler
type Service struct {
	Hostname string
	Port     int
	Nodes    []common.Node
	Mutex    sync.Mutex
}

// Arguments for a MapTask RPC call
type MapArgs struct {
	Task *MapTask
	Data []string
}

func (server *Service) HandleNodeJoin(node *common.Node) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	if node != nil && common.IsSDFSNode(*node) {
		server.Nodes = append(server.Nodes, *node)
	}
}

func (server *Service) HandleNodeLeave(node *common.Node) {
	if node != nil && common.IsSDFSNode(*node) {
		for i, sdfsNode := range server.Nodes {
			if sdfsNode.ID == node.ID {
				server.Nodes = common.RemoveIndex(server.Nodes, i)
				return
			}
		}
	}
}

func NewService(Hostname string, Port int) *Service {
	service := new(Service)
	service.Nodes = make([]common.Node, 0)
	service.Hostname = Hostname
	service.Port = Port
	return service
}

func (service *Service) Start() {
	if err := rpc.Register(service); err != nil {
		log.Fatal(err)
	}

	addr := common.GetAddress(service.Hostname, service.Port)

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

	writer := client.NewByteWriter()

	for key, value := range WordCountMapper(args.Data) {
		fname := args.Task.Param.OutputPrefix + "_" + common.EncodeFilename(key)
		log.Println(fname, key, value)
		if err := writer.Write([]byte(fmt.Sprintf("%s:%d\n", key, value))); err != nil {
			return err
		}
	}

	fmt.Println(writer.String())

	*reply = true
	return nil
}
