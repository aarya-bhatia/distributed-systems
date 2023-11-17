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
	SDFSNodes []common.Node
}

type Job interface {
	Name() string
	Run(server *Leader) error
}

type TaskData interface{}

type Task interface {
	Start(worker int, data TaskData) bool
	Restart(worker int) bool
}

func NewLeader(info common.Node) *Leader {
	leader := new(Leader)
	leader.Info = info
	leader.Scheduler = NewScheduler()
	leader.Jobs = make([]Job, 0)
	leader.CV = *sync.NewCond(&leader.Mutex)

	return leader
}

func (server *Leader) HandleNodeJoin(node *common.Node) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	if node == nil {
		return
	} else if common.IsSDFSNode(*node) {
		log.Debug("SDFS Node joined: ", *node)
		server.SDFSNodes = append(server.SDFSNodes, *node)
	} else if common.IsMapleJuiceNode(*node) {
		log.Println("MapleJuice Node joined:", *node)
		server.Scheduler.AddWorker(node.ID)
	}
}

func (server *Leader) HandleNodeLeave(node *common.Node) {
	if node == nil {
		return
	} else if common.IsSDFSNode(*node) {
		log.Println("SDFS Node left:", *node)
		for i, sdfsNode := range server.SDFSNodes {
			if sdfsNode.ID == node.ID {
				server.SDFSNodes = common.RemoveIndex(server.SDFSNodes, i)
				return
			}
		}
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

func (server *Leader) MapleRequest(args *MapParam, reply *bool) error {
	server.Mutex.Lock()

	if len(server.SDFSNodes) == 0 {
		return errors.New("No SDFS nodes are available")
	}

	serverNode := common.RandomChoice(server.SDFSNodes)
	sdfsClient := client.NewSDFSClient(common.GetAddress(serverNode.Hostname, serverNode.RPCPort))

	server.Mutex.Unlock()

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

// TODO: JuiceRequest
