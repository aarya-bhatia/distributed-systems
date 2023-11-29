package maplejuice

import (
	"cs425/common"
	"cs425/failuredetector"
	"cs425/filesystem/client"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const EXECUTOR_POLL_INTERVAL = 20 * time.Millisecond

const (
	RPC_START_MAP_JOB     = "Service.StartMapJob"
	RPC_START_REDUCE_JOB  = "Service.StartReduceJob"
	RPC_FINISH_MAP_JOB    = "Service.FinishMapJob"
	RPC_FINISH_REDUCE_JOB = "Service.FinishReduceJob"

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

	FD *failuredetector.Server

	NumExecutor int
	Tasks       []Message
	Data        map[string][]string
}

func NewService(info common.Node) *Service {
	service := new(Service)
	service.ID = info.ID
	service.Nodes = make([]common.Node, 0)
	service.Hostname = info.Hostname
	service.Port = info.RPCPort
	service.Tasks = make([]Message, 0)
	service.Data = make(map[string][]string)
	service.FD = failuredetector.NewServer(info.Hostname, info.UDPPort, common.GOSSIP_PROTOCOL, service)

	return service
}

func (service *Service) Start() {
	log.Info("Starting MapleJuice worker")
	go common.StartRPCServer(service.Hostname, service.Port, service)
	time.Sleep(time.Second)
	go service.FD.Start()
}

func merge(m1 map[string][]string, m2 map[string][]string) map[string][]string {
	m := m1
	for k, v := range m2 {
		m[k] = append(m[k], v...)
	}
	return m
}

func (service *Service) allocate(size int) {
	service.Mutex.Lock()
	defer service.Mutex.Unlock()

	for i := 0; i < size; i++ {
		go service.StartExecutor()
	}

	log.Println("Allocated", size, "workers")
	service.NumExecutor += size
}

func (service *Service) AddTask(task Task) {
	service.Mutex.Lock()
	defer service.Mutex.Unlock()
	service.Tasks = append(service.Tasks, Message{Task: task, Finish: false})
}

func (service *Service) MapTask(args *MapTask, reply *bool) error {
	service.AddTask(args)
	return nil
}

func (service *Service) ReduceTask(args *ReduceTask, reply *bool) error {
	service.AddTask(args)
	return nil
}

func (service *Service) free() {
	service.Mutex.Lock()
	defer service.Mutex.Unlock()

	for i := 0; i < service.NumExecutor; i++ {
		service.Tasks = append(service.Tasks, Message{Task: nil, Finish: true})
	}

	log.Println("Deallocated", service.NumExecutor, "workers")
	service.NumExecutor = 0
}

func (server *Service) StartMapJob(job *MapJob, reply *bool) error {
	log.Println("StartMapJob()")
	sdfsClient, err := server.getSDFSClient()
	if err != nil {
		return err
	}
	server.free()
	server.allocate(job.NumMapper)
	return server.downloadExecutable(sdfsClient, job.MapperExe)
}

func (server *Service) StartReduceJob(job *ReduceJob, reply *bool) error {
	log.Println("StartReduceJob()")
	sdfsClient, err := server.getSDFSClient()
	if err != nil {
		return err
	}
	server.free()
	server.allocate(job.NumReducer)
	return server.downloadExecutable(sdfsClient, job.ReducerExe)
}

func (server *Service) FinishMapJob(job *MapJob, reply *bool) error {
	log.Println("FinishMapJob()")

	go func() {
		defer server.free()

		sdfsClient, err := server.getSDFSClient()
		if err != nil {
			log.Fatal(err)
		}

		server.Mutex.Lock()
		defer server.Mutex.Unlock()

		for key, values := range server.Data {
			outputFile := fmt.Sprintf("%s:%d:%s", job.OutputPrefix, server.ID, key)
			lines := strings.Join(values, "\n") + "\n"
			err := sdfsClient.WriteFile(client.NewByteReader([]byte(lines)), outputFile, common.FILE_APPEND)
			if err != nil {
				log.Fatal(err)
			}
		}

		server.Data = make(map[string][]string)

		conn, err := common.Connect(common.MAPLE_JUICE_LEADER_ID, common.MapleJuiceCluster)
		if err != nil {
			log.Fatal(err)
		}

		defer conn.Close()

		reply := false
		if err = conn.Call(RPC_WORKER_ACK, &server.ID, &reply); err != nil {
			log.Fatal(err)
		}

	}()

	return nil
}

func (server *Service) FinishReduceJob(job *ReduceJob, reply *bool) error {
	log.Println("FinishReduceJob()")

	go func() {
		defer server.free()

		sdfsClient, err := server.getSDFSClient()
		if err != nil {
			log.Fatal(err)
		}

		server.Mutex.Lock()
		defer server.Mutex.Unlock()

		data := ""
		for key, values := range server.Data {
			for _, value := range values {
				data += key + ":" + value + "\n"
			}
		}

		if len(data) > 0 {
			reader := client.NewByteReader([]byte(data))
			outputFile := fmt.Sprintf("%s:%d", job.OutputFile, server.ID)
			err = sdfsClient.WriteFile(reader, outputFile, common.FILE_APPEND)
			if err != nil {
				log.Fatal(err)
			}
		}

		server.Data = make(map[string][]string)

		conn, err := common.Connect(common.MAPLE_JUICE_LEADER_ID, common.MapleJuiceCluster)
		if err != nil {
			log.Fatal(err)
		}

		defer conn.Close()

		reply := false
		if err = conn.Call(RPC_WORKER_ACK, &server.ID, &reply); err != nil {
			log.Fatal(err)
		}

	}()

	return nil
}

func (server *Service) downloadExecutable(sdfsClient *client.SDFSClient, filename string) error {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	fileWriter, err := client.NewFileWriterWithOpts(filename, client.DEFAULT_FILE_FLAGS, 0777)
	if err != nil {
		log.Warn(err)
		return err
	}

	if err := sdfsClient.DownloadFile(fileWriter, filename); err != nil {
		log.Warn(err)
		return err
	}

	return nil
}

func (server *Service) StartExecutor() error {
	// Connect to leader
	conn, err := common.Connect(common.MAPLE_JUICE_LEADER_ID, common.MapleJuiceCluster)
	if err != nil {
		return err
	}
	defer conn.Close()

	sdfsClient, err := server.getSDFSClient()
	if err != nil {
		return err
	}

	log.Println("Executor started")

	for {
		server.Mutex.Lock()
		if len(server.Tasks) == 0 {
			server.Mutex.Unlock()
			time.Sleep(EXECUTOR_POLL_INTERVAL)
			continue
		}

		message := server.Tasks[0]
		server.Tasks = server.Tasks[1:]
		server.Mutex.Unlock()

		if message.Finish {
			log.Println("Executor finished")
			return nil
		}

		res, err := message.Task.Run(sdfsClient)
		if err != nil {
			log.Fatal(err)
		}

		if res != nil {
			server.Mutex.Lock()
			server.Data = merge(server.Data, res)
			server.Mutex.Unlock()
		}

		reply := false
		if err = conn.Call(RPC_WORKER_ACK, &server.ID, &reply); err != nil {
			log.Fatal(err)
		}

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
