package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const EXECUTOR_POLL_INTERVAL = 100 * time.Millisecond

const (
	RPC_ALLOCATE    = "Service.Allocate"
	RPC_FREE        = "Service.Free"
	RPC_MAP_TRASK   = "Service.MapTask"
	RPC_REDUCE_TASK = "Service.ReduceTask"
)

type Message struct {
	Task   Task
	Finish bool
}

type Service struct {
	ID       int
	Hostname string
	Port     int
	Nodes    []common.Node
	Mutex    sync.Mutex

	NumExecutor int
	Tasks       []Message
}

func NewService(ID int, Hostname string, Port int) *Service {
	service := new(Service)
	service.ID = ID
	service.Nodes = make([]common.Node, 0)
	service.Hostname = Hostname
	service.Port = Port
	service.Tasks = make([]Message, 0)

	return service
}

func (service *Service) Start() {
	log.Info("Starting MapleJuice worker")
	common.StartRPCServer(service.Hostname, service.Port, service)
}

func (service *Service) Allocate(args *int, reply *bool) error {
	service.Mutex.Lock()
	defer service.Mutex.Unlock()

	for i := 0; i < *args; i++ {
		go service.StartExecutor()
	}
	log.Println("Allocated", *args, "workers")

	service.NumExecutor += *args
	return nil
}

func (service *Service) MapTask(args *MapTask, reply *bool) error {
	service.Mutex.Lock()
	defer service.Mutex.Unlock()
	service.Tasks = append(service.Tasks, Message{Task: args, Finish: false})
	log.Println("Task added")
	return nil
}

func (service *Service) ReduceTask(args *ReduceTask, reply *bool) error {
	service.Mutex.Lock()
	defer service.Mutex.Unlock()
	service.Tasks = append(service.Tasks, Message{Task: args, Finish: false})
	log.Println("Task added")
	return nil
}

func (service *Service) Free(args *int, reply *bool) error {
	service.Mutex.Lock()
	defer service.Mutex.Unlock()

	for i := 0; i < service.NumExecutor; i++ {
		service.Tasks = append(service.Tasks, Message{Task: nil, Finish: true})
	}

	log.Println("Deallocated", service.NumExecutor, "workers")
	service.NumExecutor = 0

	return nil
}

func (server *Service) StartExecutor() error {
	// Connect to leader
	conn, err := common.Connect(common.MAPLE_JUICE_LEADER_ID, common.MapleJuiceCluster)
	if err != nil {
		return err
	}
	defer conn.Close()

	client, err := server.getSDFSClient()
	if err != nil {
		return err
	}

	for {
		server.Mutex.Lock()

		var message *Message = nil

		if len(server.Tasks) > 0 {
			message = &server.Tasks[0]
			server.Tasks = server.Tasks[1:]
		}

		server.Mutex.Unlock()

		if message != nil {
			if message.Finish {
				log.Println("Executor finished")
				return nil
			} else {
				log.Println("Task started")
				err := message.Task.Run(client)
				if err != nil {
					log.Fatal(err)
				}

				log.Debug("Task finished")
				args := WorkerAck{WorkerID: server.ID, TaskID: message.Task.GetID(), TaskStatus: true}
				err = conn.Call(RPC_WORKER_ACK, &args, new(bool))
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		time.Sleep(EXECUTOR_POLL_INTERVAL)
	}
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

func (service *Service) getSDFSClient() (*client.SDFSClient, error) {
	service.Mutex.Lock()
	defer service.Mutex.Unlock()

	if len(service.Nodes) == 0 {
		return nil, errors.New("No SDFS nodes are available")
	}
	serverNode := common.RandomChoice(service.Nodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(serverNode.Hostname, serverNode.RPCPort))
	return sdfsClient, nil
}
