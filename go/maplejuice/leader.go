package maplejuice

import (
	"bufio"
	"cs425/common"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const MalformedRequest = "ERROR Malformed Request"
const MAPLE_CHUNK_LINE_COUNT = 100

type Leader struct {
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

type TaskData interface {
}

type Task interface {
	Start(worker string, data TaskData) bool
	Restart(worker string) bool
}

func (server *Leader) HandleNodeJoin(node *common.Node) {
	server.Scheduler.AddWorker(common.GetAddress(node.Hostname, node.TCPPort))
}

func (server *Leader) HandleNodeLeave(node *common.Node) {
	server.Scheduler.RemoveWorker(common.GetAddress(node.Hostname, node.TCPPort))
}

func NewLeader(info common.Node) *Leader {
	leader := new(Leader)
	leader.Info = info
	leader.Scheduler = NewScheduler()
	leader.Jobs = make([]Job, 0)
	leader.CV = *sync.NewCond(&leader.Mutex)

	return leader
}

func (server *Leader) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Info.TCPPort))
	if err != nil {
		log.Fatal("Error starting server: ", err)
	}

	log.Infof("MapleJuice leader is running at %s:%d...\n", server.Info.Hostname, server.Info.TCPPort)

	go server.runJobs()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go server.handleConnection(conn)
	}
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

func (server *Leader) handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	request, err := reader.ReadString('\n')
	if err != nil {
		log.Println(err)
		return
	}

	request = request[:len(request)-1]
	log.Println(request)
	tokens := strings.Split(request, " ")
	verb := tokens[0]

	switch verb {
	case "maple":
		server.handleMapleRequest(conn, tokens)
	case "juice":
		server.handleJuiceRequest(conn, tokens)
	default:
		common.SendMessage(conn, "ERROR Unknown request")
		conn.Close()
	}
}

// USAGE: maple maple_exe num_maples sdfs_prefix sdfs_src_dir
func (server *Leader) handleMapleRequest(conn net.Conn, tokens []string) bool {
	if len(tokens) < 5 {
		common.SendMessage(conn, MalformedRequest)
		return false
	}

	maple_exe := tokens[1]
	num_maples, err := strconv.Atoi(tokens[2])
	if err != nil {
		common.SendMessage(conn, MalformedRequest)
		return false
	}

	sdfs_prefix := tokens[3]
	sdfs_src_dir := tokens[4]

	// _, err = os.Stat(maple_exe)
	// if err != nil {
	// 	log.Println(err)
	// 	common.SendMessage(conn, "ERROR maple_exe not found")
	// 	return false
	// }

	log.Println(maple_exe, num_maples, sdfs_prefix, sdfs_src_dir)

	// inputFiles := server.listDirectory(sdfs_src_dir)
	inputFiles := []string{} // TODO

	job := new(MapJob)
	job.ID = time.Now().UnixNano()
	job.Client = conn
	job.InputFiles = inputFiles
	job.OutputPrefix = sdfs_prefix
	job.MapperExe = maple_exe
	job.NumMapper = num_maples

	server.addJob(job)

	return true
}

func (server *Leader) handleJuiceRequest(conn net.Conn, tokens []string) {
}
