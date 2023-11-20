package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Job interface {
	Name() string
	GetTasks(sdfsClient *client.SDFSClient) ([]Task, error)
	GetNumWorkers() int
}

type Task interface {
	Run(sdfsClient *client.SDFSClient) error
	Hash() int
	GetID() int64
}

type Worker struct {
	NumExecutors int
	Tasks        []Task
}

type Leader struct {
	ID       int
	Info     common.Node
	Mutex    sync.Mutex
	JobCV    sync.Cond
	TaskCV   sync.Cond
	Jobs     []Job
	Status   int
	Nodes    []common.Node
	NumTasks int

	Workers map[int]*Worker
	Pool    *common.ConnectionPool
}

type WorkerAck struct {
	WorkerID   int
	TaskID     int64
	TaskStatus bool
}

const RPC_WORKER_ACK = "Leader.WorkerAck"
const RPC_MAPLE_REQUEST = "Leader.MapleRequest"
const RPC_JUICE_REQUEST = "Leader.JuiceRequest"

func NewLeader(info common.Node) *Leader {
	leader := new(Leader)
	leader.Info = info
	leader.Jobs = make([]Job, 0)
	leader.Workers = make(map[int]*Worker)
	leader.TaskCV = *sync.NewCond(&leader.Mutex)
	leader.JobCV = *sync.NewCond(&leader.Mutex)
	leader.Pool = common.NewConnectionPool(common.MapleJuiceCluster)

	for _, node := range common.MapleJuiceCluster {
		leader.Workers[node.ID] = &Worker{NumExecutors: 0, Tasks: make([]Task, 0)}
	}

	return leader
}

func (server *Leader) HandleNodeJoin(node *common.Node) {
	if node == nil {
		return
	}

	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	defer log.Println("Nodes:", server.Nodes)

	server.Nodes = append(server.Nodes, *node)

	if common.IsSDFSNode(*node) {
		log.Debug("SDFS Node joined: ", *node)
	} else if common.IsMapleJuiceNode(*node) {
		log.Println("MapleJuice Node joined:", *node)
	}
}

func (server *Leader) HandleNodeLeave(node *common.Node) {
	if node == nil {
		return
	}

	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	defer log.Println("Nodes:", server.Nodes)

	for i, prev := range server.Nodes {
		if prev == *node {
			server.Nodes = common.RemoveIndex(server.Nodes, i)
			break
		}
	}

	if common.IsSDFSNode(*node) {
		log.Println("SDFS Node left:", *node)
	} else if common.IsMapleJuiceNode(*node) {
		log.Println("MapleJuice Node left:", *node)
		if worker, ok := server.Workers[node.ID]; !ok {
			server.Allocate(worker.NumExecutors)
			for _, task := range worker.Tasks {
				server.AssignTask(task)
			}
			delete(server.Workers, node.ID)
		}
	}
}

func (server *Leader) Start() {
	log.Infof("MapleJuice leader is running at %s:%d...\n", server.Info.Hostname, server.Info.RPCPort)
	go server.runJobs()
	go common.StartRPCServer(server.Info.Hostname, server.Info.RPCPort, server)
}

func (server *Leader) addJob(job Job) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	server.Jobs = append(server.Jobs, job)
	log.Info("job added:", job.Name())
	server.JobCV.Broadcast()
}

func (server *Leader) runJobs() {
	for {
		log.Println("waiting for jobs...")
		server.Mutex.Lock()
		for len(server.Jobs) == 0 {
			server.JobCV.Wait()
		}
		job := server.Jobs[0]
		server.Jobs = server.Jobs[1:]
		server.Mutex.Unlock()

		log.Println("job started:", job.Name())

		server.Pool.Close() // Reset pool
		server.Allocate(job.GetNumWorkers())

		sdfsNode := common.RandomChoice(server.GetSDFSNodes())
		sdfsClient := client.NewSDFSClient(common.GetAddress(sdfsNode.Hostname, sdfsNode.RPCPort))
		tasks, err := job.GetTasks(sdfsClient)
		if err != nil {
			log.Println("job failed:", err)
			server.Free()
			continue
		}

		for _, task := range tasks {
			server.AssignTask(task)
			log.Println("Map task scheduled:", task)
		}

		server.Wait()
		server.Free()
		log.Println("job finished:", job.Name())
	}
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

	server.addJob(&ReduceJob{
		ID:         time.Now().UnixNano(),
		Param:      *args,
		InputFiles: *inputFiles,
	})

	return nil
}

func (server *Leader) WorkerAck(args *WorkerAck, reply *bool) error {
	server.TaskDone(args.WorkerID, args.TaskID, args.TaskStatus)
	return nil
}

func (s *Leader) AssignTask(task Task) {
	workers := s.GetMapleJuiceNodes()
	worker := workers[task.Hash()%len(workers)]

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	s.Workers[worker.ID].Tasks = append(s.Workers[worker.ID].Tasks, task)
	s.NumTasks++
	log.Println("Task", task, "assigned to worker", worker)

	conn, err := s.Pool.GetConnection(worker.ID)
	if err != nil {
		log.Warn(err)
		return
	}

	reply := false
	switch task.(type) {
	case *MapTask:
		if err := conn.Call(RPC_MAP_TRASK, task.(*MapTask), &reply); err != nil {
			log.Println(err)
		}
	case *ReduceTask:
		if err := conn.Call(RPC_REDUCE_TASK, task.(*ReduceTask), &reply); err != nil {
			log.Println(err)
		}
	default:
		log.Fatal("Invalid type of task")
	}
}

func (s *Leader) TaskDone(workerID int, taskID int64, status bool) {
	s.Mutex.Lock()
	log.Printf("Ack (%v) from worker %d for task %d", status, workerID, taskID)
	worker := s.Workers[workerID]

	for i, task := range worker.Tasks {
		if task.GetID() == taskID {
			worker.Tasks = common.RemoveIndex(worker.Tasks, i)
			if status {
				s.NumTasks--
				s.TaskCV.Broadcast()
				s.Mutex.Unlock()
				return
			} else {
				s.Mutex.Unlock()
				s.AssignTask(task)
				return
			}
		}
	}

	s.Mutex.Unlock()
}

func (server *Leader) Wait() {
	log.Println("waiting for tasks to finish...")
	for {
		server.Mutex.Lock()
		if server.NumTasks == 0 {
			server.Mutex.Unlock()
			return
		}
		server.Mutex.Unlock()
		time.Sleep(time.Second)
	}
}

func (server *Leader) Allocate(size int) {
	nodes := server.GetMapleJuiceNodes()
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	for _, node := range nodes {
		conn, err := server.Pool.GetConnection(node.ID)
		if err != nil {
			log.Println(err)
			continue
		}
		args := size // TODO
		reply := false
		log.Println("allocating", size, "workers to node", node.ID)
		if err := conn.Call(RPC_ALLOCATE, &args, &reply); err != nil {
			log.Println(err)
		}
	}
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
		}
		worker.NumExecutors = 0
	}
}
