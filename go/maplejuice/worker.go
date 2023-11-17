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

func (service *Service) MapTask(args *MapTask, reply *bool) error {
	log.Println("Recevied map task:", args)

	service.Mutex.Lock()
	if len(service.Nodes) == 0 {
		service.Mutex.Unlock()
		return errors.New("No SDFS nodes are available")
	}
	serverNode := common.RandomChoice(service.Nodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(serverNode.Hostname, serverNode.RPCPort))
	service.Mutex.Unlock()

	writer := client.NewByteWriter()
	if err := sdfsClient.ReadFile(writer, args.Filename, args.Offset, args.Length); err != nil {
		return err
	}

	lines := strings.Split(writer.String(), "\n")

	res, err := WordCountMapper(lines)
	if err != nil {
		return err
	}

	for key, value := range res {
		filename := args.Param.OutputPrefix + "_" + common.EncodeFilename(key)
		data := fmt.Sprintf("%s:%d\n", key, value)
		if err := sdfsClient.WriteFile(client.NewByteReader([]byte(data)), filename, common.FILE_APPEND); err != nil {
			return err
		}
	}

	*reply = true
	return nil
}
