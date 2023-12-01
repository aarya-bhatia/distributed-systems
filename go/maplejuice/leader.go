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

const RESET_CONNECTION_TIMEOUT = 120 * time.Second
const THREAD_POOL_SIZE = 4
const WAIT_INTERVAL = 100 * time.Millisecond
const MAX_JOB_RETRY = 3
const JOB_RETRY_INTERVAL = 1000 * time.Millisecond

type Worker struct {
	ID           int
	NumExecutors int
	Tasks        []Task
	NumAcks      int
}

type JobStat struct {
	Job        Job
	StartTime  int64
	EndTime    int64
	Status     bool
	NumWorkers int
	NumTasks   int
}

type Leader struct {
	ID          int
	Info        common.Node
	Mutex       sync.Mutex
	FD          *failuredetector.Server
	Jobs        []Job
	Nodes       []common.Node
	Workers     map[int]*Worker
	NumTasks    int
	Tasks       chan Task
	JobFailure  chan bool
	NodeFailure []chan int
	JobStats    []*JobStat
}

const RPC_WORKER_ACK = "Leader.WorkerAck"
const RPC_MAPLE_REQUEST = "Leader.MapleRequest"
const RPC_JUICE_REQUEST = "Leader.JuiceRequest"

// Initialise instance of leader server
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
	leader.JobStats = make([]*JobStat, 0)

	leader.NodeFailure = make([]chan int, THREAD_POOL_SIZE)
	for i := 0; i < THREAD_POOL_SIZE; i++ {
		leader.NodeFailure[i] = make(chan int)
	}

	return leader
}

// Returns list of worker node IDs
func (s *Leader) getAvailableWorkers() []int {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return common.GetKeys(s.Workers)
}

// Returns a SDFS client using any available SDFS server node if any
func (s *Leader) getSDFSClient() (*client.SDFSClient, error) {
	sdfsNodes := s.GetSDFSNodes()
	if len(sdfsNodes) == 0 {
		return nil, errors.New("No SDFS nodes available")
	}

	serverNode := common.RandomChoice(sdfsNodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(serverNode.Hostname, serverNode.RPCPort))

	return sdfsClient, nil
}

// Add a new node
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

// Remove the failed node from list
func (server *Leader) removeNode(node common.Node) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	for i, prev := range server.Nodes {
		if prev.ID == node.ID {
			server.Nodes = common.RemoveIndex(server.Nodes, i)
			break
		}
	}

	for i := 0; i < THREAD_POOL_SIZE; i++ {
		server.NodeFailure[i] <- node.ID
	}
}

// To handle failure of worker node and reschedule its tasks elsewhere
func (server *Leader) removeWorker(workerID int) {
	server.Mutex.Lock()
	worker, ok := server.Workers[workerID]
	if !ok {
		server.Mutex.Unlock()
		return
	}
	delete(server.Workers, workerID)
	server.Mutex.Unlock()

	if len(server.Jobs) > 0 {
		server.cleanupWorker(server.Jobs[0], workerID)
	}

	log.Warn("Rescheduling tasks of worker", workerID)

	for _, task := range worker.Tasks {
		server.Tasks <- task
	}

	server.Mutex.Lock()
	server.NumTasks -= len(worker.Tasks)
	server.NumTasks += worker.NumAcks

	server.Mutex.Unlock()
}

// Callback for node join
func (server *Leader) HandleNodeJoin(node *common.Node) {
	if node == nil {
		return
	}
	server.addNode(*node)

	if common.IsSDFSNode(*node) {
		log.Println("SDFS Node joined: ", *node)
	} else if common.IsMapleJuiceNode(*node) {
		log.Println("MapleJuice Node joined:", *node)
	}
}

// Callback for node failure
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

// Starts RPC server, failure detector and other routines
func (server *Leader) Start() {
	log.Infof("MapleJuice leader is running at %s:%d...\n", server.Info.Hostname, server.Info.RPCPort)

	go common.StartRPCServer(server.Info.Hostname, server.Info.RPCPort, server)

	for i := 0; i < THREAD_POOL_SIZE; i++ {
		go server.scheduler(i)
	}

	go server.runJobs()
	go server.FD.Start()
}

// Returns the current sdfs nodes
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

// Returns the current maplejuice nodes
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

// Add map job
func (server *Leader) MapleRequest(args *MapJob, reply *bool) error {
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

	if _, err := sdfsClient.GetFile(args.MapperExe); err != nil {
		return err
	}

	server.addJob(args)
	return nil
}

// Add reduce job
func (server *Leader) JuiceRequest(args *ReduceJob, reply *bool) error {
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

	if _, err := sdfsClient.GetFile(args.ReducerExe); err != nil {
		return err
	}

	server.addJob(args)
	return nil
}

// RPC function to confirm the completion of a task by a worker
func (server *Leader) WorkerAck(args *int, reply *bool) error {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	if worker, ok := server.Workers[*args]; ok {
		worker.NumAcks++
		server.NumTasks--
	}
	return nil
}

// Send task to given worker
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

// Attemps to send task to any one worker
func (s *Leader) schedule(task Task, pool *common.ConnectionPool) bool {
	available := s.getAvailableWorkers()

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
			pool.RemoveConnection(assignee)
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

// Sends available tasks to worker nodes
func (s *Leader) scheduler(i int) {
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
			pool.Close()

		case ID := <-s.NodeFailure[i]:
			pool.RemoveConnection(ID)
		}
	}
}

// Enqueue a new job
func (server *Leader) addJob(job Job) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	server.Jobs = append(server.Jobs, job)
	log.Info("job added:", job.Name())
}

func (s *Leader) addStat(jobStat *JobStat) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	log.Println("Added job stat:", *jobStat)
	s.JobStats = append(s.JobStats, jobStat)
}

// Run each job in the order they arrive
func (server *Leader) runJobs() {
	for {
		server.Mutex.Lock()

		for len(server.Jobs) > 0 {
			job := server.Jobs[0]
			server.Mutex.Unlock()

			for i := 0; i < MAX_JOB_RETRY; i++ {
				log.Warn("job started:", job.Name())
				stat, err := server.runJob(job)

				if err != nil {
					log.Warn("job failed:", err)
					stat.Status = false
					server.addStat(stat)
				} else {
					log.Warn("job finished:", job.Name())
					stat.Status = true
					server.addStat(stat)
					break
				}

				log.Println("Retrying job...")
				time.Sleep(JOB_RETRY_INTERVAL)
			}

			server.Mutex.Lock()
			server.Jobs = server.Jobs[1:]
		}

		server.Mutex.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

// Run the current job
func (s *Leader) runJob(job Job) (*JobStat, error) {
	stat := &JobStat{Job: job}
	stat.StartTime = time.Now().UnixNano()
	stat.NumWorkers = job.GetNumWorkers()

	defer func() {
		stat.EndTime = time.Now().UnixNano()
	}()

	// delete files from previous runs
	if err := s.cleanup(job); err != nil {
		return stat, err
	}

	sdfsClient, err := s.getSDFSClient()
	if err != nil {
		return stat, err
	}

	// initialises and allocate executors at each worker node
	available := s.startJob(job)

	if len(available) == 0 {
		return stat, errors.New("No workers are available")
	}

	tasks, err := job.GetTasks(sdfsClient)
	if err != nil {
		return stat, err
	}

	stat.NumTasks = len(tasks)

	// send all tasks
	for _, task := range tasks {
		s.Tasks <- task
	}

	if !s.wait() {
		return stat, errors.New("failed during wait()")
	}

	// uploads all output files from worker nodes to sdfs
	if err = s.finishJob(job); err != nil {
		// TODO: Don't fail job, try rescheduling the tasks
		s.wait()
		return stat, err
	}

	if !s.wait() {
		return stat, errors.New("failed during wait()")
	}

	return stat, nil
}

// Initialise and allocate executors at each worker node.
// Returns the worker nodes available
func (s *Leader) startJob(job Job) []int {
	log.Println("startJob()")
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

		switch job.(type) {
		case *MapJob:
			if err := conn.Call(RPC_START_MAP_JOB, job.(*MapJob), &reply); err != nil {
				log.Println(err)
				continue
			}
		case *ReduceJob:
			if err := conn.Call(RPC_START_REDUCE_JOB, job.(*ReduceJob), &reply); err != nil {
				log.Println(err)
				continue
			}
		}

		s.Workers[node.ID] = &Worker{ID: node.ID, Tasks: make([]Task, 0), NumExecutors: job.GetNumWorkers()}
		available = append(available, node.ID)
		log.Println("Started job at worker", node.ID)
	}

	return available
}

// Flush the final map/reduce data from workers to SDFS
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
			if err := conn.Call(RPC_FINISH_MAP_JOB, job.(*MapJob), &reply); err != nil {
				log.Println(err)
				return err
			}
		case *ReduceJob:
			if err := conn.Call(RPC_FINISH_REDUCE_JOB, job.(*ReduceJob), &reply); err != nil {
				log.Println(err)
				return err
			}
		}

		log.Println("Finishing job at worker", worker.ID)
		server.NumTasks++
	}

	return nil
}

// Wait for all tasks to finish and return true only if all tasks were successfull
func (s *Leader) wait() bool {
	log.Println("Waiting for tasks to finish...")
	status := true
	for {
		select {
		case <-s.JobFailure:
			status = false

		case <-time.After(WAIT_INTERVAL):
			s.Mutex.Lock()
			n := s.NumTasks
			s.Mutex.Unlock()
			if n == 0 {
				return status
			}
		}
	}
}

func (s *Leader) cleanup(job Job) error {
	log.Println("cleanup()")

	sdfsClient, err := s.getSDFSClient()
	if err != nil {
		return err
	}

	switch job.(type) {
	case *MapJob:
		// delete prefix files
		if err := sdfsClient.DeleteAll(job.(*MapJob).OutputPrefix); err != nil {
			return err
		}

	case *ReduceJob:
		// delete output file
		if err := sdfsClient.DeleteAll(job.(*ReduceJob).OutputFile); err != nil {
			return err
		}
	}

	return nil
}

// remove temporary files generated by worker on failure
func (s *Leader) cleanupWorker(job Job, worker int) error {
	log.Println("cleanupWorker(", worker, ")")

	var list *[]string
	var err error

	sdfsClient, err := s.getSDFSClient()
	if err != nil {
		return err
	}

	switch job.(type) {
	case *MapJob:
		list, err = sdfsClient.ListDirectory(job.(*MapJob).OutputPrefix)

	case *ReduceJob:
		list, err = sdfsClient.ListDirectory(job.(*ReduceJob).OutputFile)
	}

	if err != nil {
		return err
	}

	for _, file := range *list {
		tokens := strings.Split(file, ":")
		if len(tokens) < 2 {
			continue
		}
		workerId, err := strconv.Atoi(tokens[1])
		if err != nil {
			log.Println(err)
			continue
		}
		if workerId == worker {
			go sdfsClient.DeleteFile(file)
		}
	}

	return nil
}
