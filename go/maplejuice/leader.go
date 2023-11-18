package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Leader struct {
	ID        int
	Info      common.Node
	Mutex     sync.Mutex
	CV        sync.Cond
	Scheduler *Scheduler
	Jobs      []Job
	Status    int
	Nodes     []common.Node
}

type Job interface {
	Name() string
	Run(server *Leader) error
}

type WorkerAck struct {
	WorkerID   int
	TaskID     int64
	TaskStatus bool
}

const RPC_MAPLE_WORKER_ACK = "Leader.MapleWorkerAck"

func NewLeader(info common.Node) *Leader {
	leader := new(Leader)
	leader.Info = info
	leader.Scheduler = NewScheduler()
	leader.Jobs = make([]Job, 0)
	leader.CV = *sync.NewCond(&leader.Mutex)

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
		server.Scheduler.AddWorker(node.ID)
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
		server.Scheduler.RemoveWorker(node.ID)
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
	server.CV.Broadcast()
}

func (server *Leader) runJobs() {
	for {
		log.Println("waiting for jobs...")
		server.Mutex.Lock()
		for len(server.Jobs) == 0 {
			server.CV.Wait()
		}
		job := server.Jobs[0]
		server.Jobs = server.Jobs[1:]
		server.Mutex.Unlock()

		log.Println("job started:", job.Name())

		if err := job.Run(server); err != nil {
			log.Println("job failed:", err)
		} else {
			log.Println("job finished:", job.Name())
		}
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

func (server *Leader) MapleWorkerAck(args *WorkerAck, reply *bool) error {
	server.Scheduler.TaskDone(args.WorkerID, args.TaskID, args.TaskStatus)
	return nil
}

// TODO: JuiceRequest
