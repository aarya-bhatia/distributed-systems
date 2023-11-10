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

	log "github.com/sirupsen/logrus"
)

const (
	WORKER_IDLE = 0
	WORKER_WAIT = 1
	WORKER_BUSY = 2
)

const (
	MAP    = 0
	REDUCE = 1
)

const (
	MalformedRequest = "ERROR Malformed Request"
)

type Job struct {
	ID         string
	MapperExe  string
	ReducerExe string
	NumMapper  int
	NumReducer int
	FilePrefix string
	InputFiles []string
	Phase      int
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

func (server *Leader) HandleNodeJoin(node *common.Node) {
	log.Println("node joined:", *node)
}

func (server *Leader) HandleNodeLeave(node *common.Node) {
	log.Println("node left:", *node)
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

	if tokens[0] == "maple" {
		if len(tokens) < 5 {
			common.SendMessage(conn, MalformedRequest)
			return
		}
		maple_exe := tokens[1]
		num_maples, err := strconv.Atoi(tokens[2])
		if err != nil {
			common.SendMessage(conn, MalformedRequest)
			return
		}
		sdfs_prefix := tokens[3]
		sdfs_src_dir := tokens[4]
		_, err = os.Stat(maple_exe)
		if err != nil {
			log.Println(err)
			common.SendMessage(conn, "ERROR maple_exe not found")
			return
		}
		log.Println(maple_exe, num_maples, sdfs_prefix, sdfs_src_dir)
	}

	common.SendMessage(conn, "OK")
}
