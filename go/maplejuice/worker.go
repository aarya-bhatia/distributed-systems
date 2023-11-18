package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

const (
	RPC_MAP_TASK    = "Service.MapTask"
	RPC_REDUCE_TASK = "Service.ReduceTask"
)

// RPC handler
type Service struct {
	ID       int
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

func NewService(ID int, Hostname string, Port int) *Service {
	service := new(Service)
	service.ID = ID
	service.Nodes = make([]common.Node, 0)
	service.Hostname = Hostname
	service.Port = Port
	return service
}

// Start rpc server
func (service *Service) Start() {
	log.Info("Starting MapleJuice worker")
	common.StartRPCServer(service.Hostname, service.Port, service)
}

// Recevie a map task from leader
func (service *Service) MapTask(args *MapTask, reply *bool) error {
	log.Println("Recevied map task:", args)
	service.StartMapTask(*args)
	return nil
}

// Recevie a reduce task from leader
func (service *Service) ReduceTask(args *ReduceTask, reply *bool) error {
	log.Println("Recevied reduce task:", args)
	service.StartReduceTask(*args)
	return nil
}

// Schedule and run map task
func (service *Service) StartMapTask(task MapTask) {
	status := false
	defer service.FinishTask(task.ID, status)

	if task.Param.NumMapper <= 0 {
		log.Println("num mappers should be a positive number")
		return
	}

	service.Mutex.Lock()
	if len(service.Nodes) == 0 {
		service.Mutex.Unlock()
		log.Println("No SDFS nodes are available")
		return
	}
	serverNode := common.RandomChoice(service.Nodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(serverNode.Hostname, serverNode.RPCPort))
	service.Mutex.Unlock()

	writer := client.NewByteWriter()
	if err := sdfsClient.ReadFile(writer, task.Filename, task.Offset, task.Length); err != nil {
		log.Println(err)
		return
	}

	lines := strings.Split(writer.String(), "\n")

	done := make(chan bool)
	go service.StartMapExecutor(task.Param, lines, done)
	log.Println("Waiting for executor...")
	status = <-done
	log.Println("Finished map task")

	// linesPerMapper := common.Max(1, int(len(lines)/task.Param.NumMapper))
	// done := make(chan bool)
	// count := 0
	//
	// log.Println("Recevied", len(lines), "lines")
	//
	// for i := 0; i < task.Param.NumMapper; i++ {
	// 	numLines := common.Min(linesPerMapper, len(lines))
	// 	if numLines == 0 {
	// 		break
	// 	}
	//
	// 	go service.StartMapExecutor(task.Param, lines[:numLines], done)
	// 	lines = lines[numLines:]
	// 	count++
	// }
	//
	// status = true
	// for i := 0; i < count; i++ {
	// 	if !(<-done) {
	// 		status = false
	// 	}
	// }
}

// Schedule and run reduce task
func (service *Service) StartReduceTask(task ReduceTask) {
	status := false
	defer service.FinishTask(task.ID, status)

	if task.Param.NumReducer <= 0 {
		log.Println("num reducers should be a positive number")
		return
	}

	service.Mutex.Lock()
	if len(service.Nodes) == 0 {
		service.Mutex.Unlock()
		log.Println("No SDFS nodes are available")
		return
	}
	serverNode := common.RandomChoice(service.Nodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(serverNode.Hostname, serverNode.RPCPort))
	service.Mutex.Unlock()

	writer := client.NewByteWriter()
	if err := sdfsClient.DownloadFile(writer, task.InputFile); err != nil {
		log.Println(err)
		return
	}

	lines := strings.Split(writer.String(), "\n")

	done := make(chan bool)
	go service.StartReduceExecutor(task.Param, lines, done)
	log.Println("Waiting for executor...")
	status = <-done
	log.Println("Finished reduce task")
}

// Send task status to leader
func (service *Service) FinishTask(taskID int64, status bool) {
	conn, err := common.Connect(common.MAPLE_JUICE_LEADER_ID, common.MapleJuiceCluster)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	args := WorkerAck{WorkerID: service.ID, TaskID: taskID, TaskStatus: true}
	err = conn.Call(RPC_WORKER_ACK, &args, new(bool))
	if err != nil {
		log.Fatal(err)
	}
}

// Run mapper and save output in sdfs
func (service *Service) StartMapExecutor(param MapParam, lines []string, done chan bool) {
	log.Println("Running mapper with", len(lines), "lines")
	result := false
	defer func() {
		done <- result
	}()

	service.Mutex.Lock()
	if len(service.Nodes) == 0 {
		service.Mutex.Unlock()
		return
	}
	serverNode := common.RandomChoice(service.Nodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(serverNode.Hostname, serverNode.RPCPort))
	service.Mutex.Unlock()

	res, err := WordCountMapper(lines)
	if err != nil {
		log.Println(err)
		return
	}

	for key, value := range res {
		filename := param.OutputPrefix + "_" + common.EncodeFilename(key)
		data := fmt.Sprintf("%s:%d\n", key, value)
		if err := sdfsClient.WriteFile(client.NewByteReader([]byte(data)), filename, common.FILE_APPEND); err != nil {
			log.Println(err)
			return
		}
	}

	result = true
}

// Run reducer and save output in sdfs
func (service *Service) StartReduceExecutor(param ReduceParam, lines []string, done chan bool) {
	log.Println("Running reducer with", len(lines), "lines")
	result := false
	defer func() {
		done <- result
	}()

	service.Mutex.Lock()
	if len(service.Nodes) == 0 {
		service.Mutex.Unlock()
		return
	}
	serverNode := common.RandomChoice(service.Nodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(serverNode.Hostname, serverNode.RPCPort))
	service.Mutex.Unlock()

	res, err := WordCountReducer(lines)
	if err != nil {
		log.Println(err)
		return
	}

	data := ""
	for k, v := range res {
		data += fmt.Sprintf("%s:%d\n", k, v)
	}

	if err := sdfsClient.WriteFile(client.NewByteReader([]byte(data)), param.OutputFile, common.FILE_APPEND); err != nil {
		log.Println(err)
		return
	}

	result = true
}
