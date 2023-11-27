package maplejuice

import (
	"cs425/common"
	"cs425/failuredetector"
	"cs425/filesystem/client"
	"errors"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const RESET_CONNECTION_TIMEOUT = 30 * time.Second
const THREAD_POOL_SIZE = 4
const WAIT_INTERVAL = 100 * time.Millisecond

type Worker struct {
	ID           int
	NumExecutors int
	Tasks        []Task
	NumAcks      int
}

type Leader struct {
	ID         int
	Info       common.Node
	Mutex      sync.Mutex
	FD         *failuredetector.Server
	Jobs       []Job
	Nodes      []common.Node
	Workers    map[int]*Worker
	NumTasks   int
	Tasks      chan Task
	JobFailure chan bool
}

const RPC_WORKER_ACK = "Leader.WorkerAck"
const RPC_MAPLE_REQUEST = "Leader.MapleRequest"
const RPC_JUICE_REQUEST = "Leader.JuiceRequest"

func (s *Leader) GetAvailableWorkers() []int {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return common.GetKeys(s.Workers)
}

func (s *Leader) getSDFSClient() (*client.SDFSClient, error) {
	sdfsNodes := s.GetSDFSNodes()
	if len(sdfsNodes) == 0 {
		return nil, errors.New("No SDFS nodes available")
	}

	serverNode := common.RandomChoice(sdfsNodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(serverNode.Hostname, serverNode.RPCPort))

	return sdfsClient, nil
}

func NewLeader(info common.Node) *Leader {
	leader := new(Leader)
	leader.Info = info
	leader.Jobs = make([]Job, 0)
	leader.Nodes = make([]common.Node, 0)
	leader.Workers = make(map[int]*Worker)
	leader.FD = failuredetector.NewServer(info.Hostname, info.UDPPort, common.GOSSIP_PROTOCOL, leader)
	leader.Tasks = make(chan Task)
	leader.JobFailure = make(chan bool)
	leader.NumTasks = 0

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

func (server *Leader) removeWorker(workerID int) {
	server.Mutex.Lock()
	worker, ok := server.Workers[workerID]
	if !ok {
		server.Mutex.Unlock()
		return
	}
	delete(server.Workers, workerID)
	server.Mutex.Unlock()

	log.Warn("Rescheduling tasks of worker", workerID)

	for _, task := range worker.Tasks {
		server.Tasks <- task
	}

	server.Mutex.Lock()
	server.NumTasks -= len(worker.Tasks)
	server.NumTasks += worker.NumAcks
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

	go common.StartRPCServer(server.Info.Hostname, server.Info.RPCPort, server)

	for i := 0; i < THREAD_POOL_SIZE; i++ {
		go server.scheduler()
	}

	go server.runJobs()
	go server.FD.Start()
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

	if args.NumMapper <= 0 {
		return errors.New("Number of workers must be greater than 0")
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

	if args.NumReducer <= 0 {
		return errors.New("Number of workers must be greater than 0")
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

	// TODO
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
	if worker, ok := server.Workers[*args]; ok {
		// log.Println("Ack")
		worker.NumAcks++
		server.NumTasks--
	}
	return nil
}

func (s *Leader) allocate(numWorkers int) []int {
	nodes := s.GetMapleJuiceNodes()

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	s.NumTasks = 0
	s.Workers = make(map[int]*Worker)
	available := []int{}
	reply := false

	for _, node := range nodes {
		conn, err := common.Connect(node.ID, common.MapleJuiceCluster)
		if err != nil {
			log.Println(err)
			continue
		}
		defer conn.Close()
		if err := conn.Call(RPC_ALLOCATE, &numWorkers, &reply); err != nil {
			log.Println(err)
			continue
		}
		log.Println("Allocated", numWorkers, "executors to worker", node.ID)
		s.Workers[node.ID] = &Worker{ID: node.ID, Tasks: make([]Task, 0), NumExecutors: numWorkers}
		available = append(available, node.ID)
	}

	return available
}

func (server *Leader) free() {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	for _, worker := range server.Workers {
		reply := false
		log.Println("deallocating workers at worker", worker.ID)
		if worker.NumExecutors == 0 {
			continue
		}
		conn, err := common.Connect(worker.ID, common.MapleJuiceCluster)
		if err != nil {
			log.Println(err)
			continue
		}
		defer conn.Close()
		if err := conn.Call(RPC_FREE, &worker.NumExecutors, &reply); err != nil {
			log.Println(err)
			continue
		}
	}
}

func assign(task Task, conn *rpc.Client) bool {
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

	return true
}

func (s *Leader) schedule(task Task, pool *common.ConnectionPool) bool {
	available := s.GetAvailableWorkers()

	for len(available) > 0 {
		hash := task.Hash() % len(available)
		assignee := available[hash]

		conn, err := pool.GetConnection(assignee)
		if err != nil {
			log.Println(err)
			available = common.RemoveElement(available, assignee)
			continue
		}

		if !assign(task, conn) {
			available = common.RemoveElement(available, assignee)
			continue
		}

		s.Mutex.Lock()
		s.Workers[assignee].Tasks = append(s.Workers[assignee].Tasks, task)
		s.NumTasks++
		s.Mutex.Unlock()

		log.Debug("Assigned task ", task.GetID(), " to worker", assignee)

		return true
	}

	return false
}

func (s *Leader) scheduler() {
	pool := common.NewConnectionPool(common.MapleJuiceCluster)
	log.Println("Started scheduler routine...")
	for {
		select {
		case task := <-s.Tasks:
			if !s.schedule(task, pool) {
				log.Warn("Failed to schedule task...")
				s.JobFailure <- true
			}

		case <-time.After(RESET_CONNECTION_TIMEOUT):
			log.Warn("Reset connection pool")
			pool.Close()
		}
	}
}

func (server *Leader) addJob(job Job) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	server.Jobs = append(server.Jobs, job)
	log.Info("job added:", job.Name())
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
func (s *Leader) runJob(job Job) error {
	defer s.free()

	available := s.allocate(job.GetNumWorkers())
	if len(available) == 0 {
		return errors.New("No workers are available")
	}

	sdfsClient, err := s.getSDFSClient()
	if err != nil {
		return err
	}

	tasks, err := job.GetTasks(sdfsClient)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		s.Tasks <- task
	}

	if !s.wait() {
		return errors.New("job failure")
	}

	err = s.finishJob(job)
	if err != nil {
		return err
	}

	if !s.wait() {
		return errors.New("job failure")
	}

	return nil
}

func (server *Leader) finishJob(job Job) error {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	log.Println("finishJob()")
	reply := false

	for _, worker := range server.Workers {
		conn, err := common.Connect(worker.ID, common.MapleJuiceCluster)
		if err != nil {
			log.Println(err)
			continue
		}

		defer conn.Close()

		switch job.(type) {
		case *MapJob:
			if err := conn.Call(RPC_FINISH_MAP_JOB, &job.(*MapJob).Param.OutputPrefix, &reply); err != nil {
				log.Println(err)
				go server.removeWorker(worker.ID)
				continue
			}
		case *ReduceJob:
			if err := conn.Call(RPC_FINISH_REDUCE_JOB, &job.(*ReduceJob).Param.OutputFile, &reply); err != nil {
				log.Println(err)
				go server.removeWorker(worker.ID)
				continue
			}
		}

		server.NumTasks++
	}

	return nil
}

func (s *Leader) wait() bool {
	log.Println("Waiting for tasks to finish...")
	for {
		select {
		case <-s.JobFailure:
			return false

		case <-time.After(WAIT_INTERVAL):
			s.Mutex.Lock()
			n := s.NumTasks
			s.Mutex.Unlock()
			if n == 0 {
				return true
			}
		}
	}
}
