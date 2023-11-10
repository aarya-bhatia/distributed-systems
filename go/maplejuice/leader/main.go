package leader

import (
	"bufio"
	"cs425/common"
	"cs425/queue"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	WORKER_IDLE = 0
	WORKER_WAIT = 1
	WORKER_BUSY = 2
)

const (
	MAPLE = 0 // map
	JUICE = 1 // reduce
)

const (
	MalformedRequest = "ERROR Malformed Request"
)

type Job struct {
	ID         int64
	MapperExe  string
	ReducerExe string
	NumMapper  int
	NumReducer int
	FilePrefix string
	InputFiles []string
	Type       int
	Keys       []string
}

type Worker struct {
	Info   common.Node
	Status int // idle, wait, busy
	Mode   int // map, reduce
	Conn   net.Conn
}

type Leader struct {
	Info     common.Node
	Jobs     *queue.Queue
	Workers  map[string]*Worker
	Mutex    sync.Mutex
	Listener net.Conn
}

func (server *Leader) Enqueuejob(job *Job) {
	server.Jobs.Push(job)
	log.Info("Job enqueued:", *job)
}

func (server *Leader) DequeueJob() *Job {
	job := server.Jobs.Pop().(*Job)
	log.Info("Job dequeued:", *job)
	return job
}

func (server *Leader) HandleNodeJoin(node *common.Node) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	log.Println("node joined:", *node)
	worker := new(Worker)
	worker.Info = *node
	worker.Status = WORKER_IDLE
	server.Workers[common.GetAddress(node.Hostname, node.TCPPort)] = worker
}

func (server *Leader) HandleNodeLeave(node *common.Node) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	log.Println("node left:", *node)
	ID := common.GetAddress(node.Hostname, node.TCPPort)
	for workerID := range server.Workers {
		if workerID == ID {
			delete(server.Workers, workerID)
			// TODO: reschedule tasks if neccessary
			return
		}
	}
}

func (server *Leader) listDirectory(name string) []string {
	res := []string{}
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", server.Info.Hostname, server.Info.FrontendPort))
	if err != nil {
		return res
	}
	if !common.SendMessage(conn, "lsdir "+name) {
		return res
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	line, err := reader.ReadString('\n')
	if err != nil {
		return res
	}
	if line[:len(line)-1] != "OK" {
		log.Println(line[:len(line)-1])
		return res
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		res = append(res, line[:len(line)-1])
	}

	log.Println("lsdir "+name+":", res)
	return res
}

func NewLeader(info common.Node) *Leader {
	leader := new(Leader)
	leader.Info = info
	leader.Workers = make(map[string]*Worker, 0)
	leader.Jobs = queue.NewQueue()
	return leader
}

func (server *Leader) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Info.TCPPort))
	if err != nil {
		log.Fatal("Error starting server: ", err)
	}

	log.Infof("MapleJuice leader is running at %s:%d...\n", server.Info.Hostname, server.Info.TCPPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go server.handleConnection(conn)
	}
}

func (server *Leader) handleConnection(conn net.Conn) {
	defer conn.Close()
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

	if verb == "maple" {
		server.handleMapleRequest(conn, tokens)
	}
}

// Usage: maple maple_exe num_maples sdfs_prefix sdfs_src_dir
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

	_, err = os.Stat(maple_exe)
	if err != nil {
		log.Println(err)
		common.SendMessage(conn, "ERROR maple_exe not found")
		return false
	}

	// TODO: check if maple_exe exists in sdfs / upload it to sdfs

	log.Println(maple_exe, num_maples, sdfs_prefix, sdfs_src_dir)

	inputFiles := server.listDirectory(sdfs_src_dir)

	job := new(Job)
	job.InputFiles = inputFiles
	job.FilePrefix = sdfs_prefix
	job.ID = time.Now().UnixNano()
	job.MapperExe = maple_exe
	job.Type = MAPLE
	job.Keys = make([]string, 0)
	job.ReducerExe = ""
	job.NumMapper = num_maples
	job.NumReducer = 0

	server.Enqueuejob(job)

	return common.SendMessage(conn, "OK")
}

func (server *Leader) handleJuiceRequest(conn net.Conn, tokens []string) {
}
