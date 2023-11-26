package maplejuice

import (
	"cs425/common"
	"cs425/failuredetector"
	"cs425/filesystem/client"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Worker struct {
	NumExecutors int
	Tasks        []Task
}

type Leader struct {
	ID       int
	Info     common.Node
	Mutex    sync.Mutex
	FD       *failuredetector.Server
	Jobs     []Job
	Nodes    []common.Node
	NumTasks int
	Workers  map[int]*Worker
	Pool     *common.ConnectionPool
}

const RPC_WORKER_ACK = "Leader.WorkerAck"
const RPC_MAPLE_REQUEST = "Leader.MapleRequest"
const RPC_JUICE_REQUEST = "Leader.JuiceRequest"

func NewLeader(info common.Node) *Leader {
	leader := new(Leader)
	leader.Info = info
	leader.Jobs = make([]Job, 0)
	leader.Workers = make(map[int]*Worker)
	leader.Pool = common.NewConnectionPool(common.MapleJuiceCluster)
	leader.FD = failuredetector.NewServer(info.Hostname, info.UDPPort, common.GOSSIP_PROTOCOL, leader)

	// for _, node := range common.MapleJuiceCluster {
	// 	leader.Workers[node.ID] = &Worker{NumExecutors: 0, Tasks: make([]Task, 0)}
	// }
	//
	return leader
}

func (server *Leader) addNode(node common.Node) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	// check if node exists
	for _, cur := range server.Nodes {
		if cur.ID == node.ID {
			return
		}
	}

	server.Nodes = append(server.Nodes, node)
}

func (server *Leader) removeNode(node common.Node) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	for i, prev := range server.Nodes {
		if prev.ID == node.ID {
			server.Nodes = common.RemoveIndex(server.Nodes, i)
			break
		}
	}
}

func (server *Leader) addWorker(workerID int) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	// check if worker exists
	if _, ok := server.Workers[workerID]; ok {
		return
	}

	server.Workers[workerID] = &Worker{NumExecutors: 0, Tasks: make([]Task, 0)}
}

func (server *Leader) removeWorker(workerID int) {
	server.Mutex.Lock()
	worker, ok := server.Workers[workerID]
	if !ok {
		server.Mutex.Unlock()
		return
	}
	numExecutors := worker.NumExecutors
	tasks := worker.Tasks
	delete(server.Workers, workerID)
	server.Mutex.Unlock()

	// Reallocate executors to another worker (TODO)
	server.Allocate(numExecutors)

	// Reassigning tasks of failed worker
	for _, task := range tasks {
		log.Println("Reassinging task", task)
		server.AssignTask(task) // Will update NumTasks
	}

	server.Mutex.Lock()
	server.NumTasks -= len(worker.Tasks) // Remove task count for failed worker
	server.Mutex.Unlock()
}

func (server *Leader) HandleNodeJoin(node *common.Node) {
	if node == nil {
		return
	}
	server.addNode(*node)

	if common.IsSDFSNode(*node) {
		log.Debug("SDFS Node joined: ", *node)
	} else if common.IsMapleJuiceNode(*node) {
		log.Println("MapleJuice Node joined:", *node)
		server.addWorker(node.ID)
	}
}

func (server *Leader) HandleNodeLeave(node *common.Node) {
	if node == nil {
		return
	}
	server.removeNode(*node)

	if common.IsSDFSNode(*node) {
		log.Println("SDFS Node left:", *node)
	} else if common.IsMapleJuiceNode(*node) {
		log.Println("MapleJuice Node left:", *node)
		server.removeWorker(node.ID)
	}
}

func (server *Leader) Start() {
	log.Infof("MapleJuice leader is running at %s:%d...\n", server.Info.Hostname, server.Info.RPCPort)
	go server.runJobs()
	go common.StartRPCServer(server.Info.Hostname, server.Info.RPCPort, server)
	time.Sleep(time.Second)
	go server.FD.Start()
}

func (server *Leader) addJob(job Job) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	server.Jobs = append(server.Jobs, job)
	log.Info("job added:", job.Name())
}

func (server *Leader) GetSDFSNodes() []common.Node {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	res := []common.Node{}
	for _, node := range server.Nodes {
		if common.IsSDFSNode(node) {
			res = append(res, node)
		}
	}
	return res
}

func (server *Leader) GetMapleJuiceNodes() []common.Node {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	res := []common.Node{}
	for _, node := range server.Nodes {
		if common.IsMapleJuiceNode(node) {
			res = append(res, node)
		}
	}
	return res
}

func (server *Leader) MapleRequest(args *MapParam, reply *bool) error {
	sdfsNodes := server.GetSDFSNodes()
	workers := server.GetMapleJuiceNodes()

	if len(sdfsNodes) == 0 {
		return errors.New("No SDFS nodes are available")
	}

	if len(workers) == 0 {
		return errors.New("No MapleJuice workers are available")
	}

	log.Println("sdfs nodes:", sdfsNodes)
	log.Println("maplejuice workers:", workers)

	serverNode := common.RandomChoice(sdfsNodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(serverNode.Hostname, serverNode.RPCPort))

	inputFiles, err := sdfsClient.ListDirectory(args.InputDir)
	if err != nil {
		return err
	}

	server.addJob(&MapJob{
		ID:         time.Now().UnixNano(),
		Param:      *args,
		InputFiles: *inputFiles,
	})

	return nil
}

func (server *Leader) JuiceRequest(args *ReduceParam, reply *bool) error {
	sdfsNodes := server.GetSDFSNodes()
	workers := server.GetMapleJuiceNodes()

	if len(sdfsNodes) == 0 {
		return errors.New("No SDFS nodes are available")
	}

	if len(workers) == 0 {
		return errors.New("No MapleJuice workers are available")
	}

	log.Println("sdfs nodes:", sdfsNodes)
	log.Println("maplejuice workers:", workers)

	serverNode := common.RandomChoice(sdfsNodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(serverNode.Hostname, serverNode.RPCPort))

	inputFiles, err := sdfsClient.ListDirectory(args.InputPrefix)
	if err != nil {
		return err
	}

	filtered := []string{}

	for _, inputFile := range *inputFiles {
		tokens := strings.Split(inputFile, ":")
		if len(tokens) < 2 { // filename:workerID:...
			continue
		}

		workerID, err := strconv.Atoi(tokens[1])
		if err != nil {
			continue
		}

		for _, w := range workers {
			if w.ID == workerID {
				filtered = append(filtered, inputFile)
			}
		}
	}

	server.addJob(&ReduceJob{
		ID:         time.Now().UnixNano(),
		Param:      *args,
		InputFiles: filtered,
	})

	return nil
}

func (server *Leader) WorkerAck(args *int, reply *bool) error {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	log.Printf("Ack from worker %d", *args)
	server.NumTasks--
	return nil
}

func (s *Leader) tryAssignTask(task Task) bool {
	workers := s.GetMapleJuiceNodes()
	if len(workers) == 0 {
		return false
	}

	worker := workers[task.Hash()%len(workers)]

	s.Mutex.Lock()
	conn, err := s.Pool.GetConnection(worker.ID)
	s.Mutex.Unlock()
	if err != nil {
		log.Warn(err)
		return false
	}

	reply := false
	switch task.(type) {
	case *MapTask:
		if err := conn.Call(RPC_MAP_TRASK, task.(*MapTask), &reply); err != nil {
			log.Println(err)
			return false
		}
	case *ReduceTask:
		if err := conn.Call(RPC_REDUCE_TASK, task.(*ReduceTask), &reply); err != nil {
			log.Println(err)
			return false
		}
	default:
		log.Fatal("Invalid type of task")
	}

	s.Mutex.Lock()
	s.Workers[worker.ID].Tasks = append(s.Workers[worker.ID].Tasks, task)
	s.NumTasks++
	s.Mutex.Unlock()

	log.Println("Task", task.GetID(), "assigned to worker", worker)
	return true
}

func (s *Leader) AssignTask(task Task) {
	for !s.tryAssignTask(task) {
		log.Println("Retrying AssignTask()...")
		time.Sleep(time.Second)
	}
}

func (server *Leader) Wait() {
	log.Println("waiting for tasks to finish...")
	for {
		server.Mutex.Lock()
		n := server.NumTasks
		server.Mutex.Unlock()

		if n == 0 {
			return
		}

		time.Sleep(time.Second)
	}
}

func (server *Leader) Allocate(size int) bool {
	nodes := server.GetMapleJuiceNodes()
	if len(nodes) == 0 {
		return false
	}

	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	for _, node := range nodes {
		conn, err := server.Pool.GetConnection(node.ID)
		if err != nil {
			log.Println(err)
			continue
		}
		args := size
		reply := false
		log.Println("allocating", size, "workers to node", node.ID)
		if err := conn.Call(RPC_ALLOCATE, &args, &reply); err != nil {
			log.Println(err)
			return false
		}
		server.Workers[node.ID].NumExecutors = size
	}
	return true
}

func (server *Leader) Free() {
	nodes := server.GetMapleJuiceNodes()
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	for _, node := range nodes {
		conn, err := server.Pool.GetConnection(node.ID)
		if err != nil {
			log.Println(err)
			continue
		}
		worker := server.Workers[node.ID]
		args := worker.NumExecutors
		reply := false
		log.Println("deallocating workers at node", node.ID)
		if err := conn.Call(RPC_FREE, &args, &reply); err != nil {
			log.Println(err)
			continue
		}
		worker.NumExecutors = 0
	}
}

func (server *Leader) reset() {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	server.Pool.Close()

	for _, worker := range server.Workers {
		worker.Tasks = make([]Task, 0)
	}

	server.NumTasks = 0
}

func (server *Leader) runJobs() {
	for {
		server.Mutex.Lock()

		for len(server.Jobs) > 0 {
			job := server.Jobs[0]
			server.Mutex.Unlock()

			log.Warn("job started:", job.Name())

			if err := server.runJob(job); err != nil {
				log.Warn("job failed:", err)
			} else {
				log.Warn("job finished:", job.Name())
			}

			server.Mutex.Lock()
			server.Jobs = server.Jobs[1:]
		}

		server.Mutex.Unlock()
		time.Sleep(time.Second)
	}
}

func (server *Leader) taskSender(tasks chan Task, done chan bool) {
	for task := range tasks {
		server.AssignTask(task)
	}

	done <- true
}

func (server *Leader) runJob(job Job) error {
	if !server.Allocate(job.GetNumWorkers()) {
		return errors.New("Failed to allocate worker")
	}

	defer server.Free()
	defer server.reset()

	sdfsNodes := server.GetSDFSNodes()
	if len(sdfsNodes) == 0 {
		return errors.New("No sdfs nodes are online")
	}

	sdfsNode := common.RandomChoice(sdfsNodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(sdfsNode.Hostname, sdfsNode.RPCPort))

	tasks, err := job.GetTasks(sdfsClient)
	if err != nil {
		return err
	}

	if len(tasks) == 0 {
		log.Println("No tasks")
		return nil
	}

	taskChan := make(chan Task)
	done := make(chan bool)

	// create thread pool to assign tasks
	for i := 0; i < len(sdfsNodes); i++ {
		go server.taskSender(taskChan, done)
	}

	// send all tasks
	for _, task := range tasks {
		taskChan <- task
	}

	// to signal no more tasks
	close(taskChan)

	// wait for all tasks to be assigned
	for i := 0; i < len(sdfsNodes); i++ {
		<-done
	}

	// wait for workers to read and process all input data
	server.Wait()

	workerNodes := server.GetMapleJuiceNodes()
	reply := false

	for _, node := range workerNodes {
		server.Mutex.Lock()
		conn, err := server.Pool.GetConnection(node.ID)
		server.Mutex.Unlock()
		if err != nil {
			return err
		}

		switch job.(type) {
		case *MapJob:
			if err = conn.Call(RPC_FINISH_MAP_JOB, &job.(*MapJob).Param.OutputPrefix, &reply); err != nil {
				return err
			}
		case *ReduceJob:
			if err = conn.Call(RPC_FINISH_REDUCE_JOB, &job.(*ReduceJob).Param.OutputFile, &reply); err != nil {
				return err
			}
		}

		server.Mutex.Lock()
		server.NumTasks++
		server.Mutex.Unlock()
	}

	// wait for workers to write all output data
	server.Wait()

	// cleanup output files
	/* switch job.(type) {
	case *MapJob:

	case *ReduceJob:
		for _, file := range job.(*ReduceJob).InputFiles {
			if err := sdfsClient.DeleteFile(file); err != nil {
				log.Println(err)
			}
		}
	} */

	return nil
}
