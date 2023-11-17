package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"errors"
	"fmt"
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
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

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
	log.Info("Starting MapleJuice worker")
	common.StartRPCServer(service.Hostname, service.Port, service)
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

	service.Mutex.Lock()
	if len(service.Nodes) == 0 {
		return errors.New("No SDFS nodes are available")
	}
	serviceNode := common.RandomChoice(service.Nodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(serviceNode.Hostname, serviceNode.RPCPort))
	service.Mutex.Unlock()

	for key, value := range WordCountMapper(args.Data) {
		filename := args.Task.Param.OutputPrefix + "_" + common.EncodeFilename(key)
		data := fmt.Sprintf("%s:%d\n", key, value)
		if err := sdfsClient.WriteFile(client.NewByteReader([]byte(data)), filename, common.FILE_APPEND); err != nil {
			return err
		}
	}

	*reply = true
	return nil
}
