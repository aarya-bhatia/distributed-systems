package maplejuice

import (
	"cs425/common"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Leader struct {
	ID        int
	Info      common.Node
	Mutex     sync.Mutex
	CV        sync.Cond
	Scheduler *Scheduler
	Jobs      []Job
	Status    int
}

type Job interface {
	Name() string
	Run(server *Leader) bool
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
	server.Scheduler.AddWorker(node.ID)
}

func (server *Leader) HandleNodeLeave(node *common.Node) {
	server.Scheduler.RemoveWorker(node.ID)
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
		job.Run(server)
		log.Println("job finished:", job.Name())
	}
}

// USAGE: maple maple_exe num_maples sdfs_prefix sdfs_src_dir
func (server *Leader) MapleRequest(args *MapParam, reply *bool) error {
	// inputFiles := server.listDirectory(sdfs_src_dir)
	// inputFiles := []string{} // TODO
	server.addJob(&MapJob{
		ID:         time.Now().UnixNano(),
		Param:      *args,
		InputFiles: []string{"./data/vm1"}, // TODO
	})

	return nil
}

// TODO: JuiceRequest
