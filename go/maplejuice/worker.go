package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const EXECUTOR_POLL_INTERVAL = 20 * time.Millisecond

const (
	RPC_START_MAP_JOB     = "Service.StartMapJob"
	RPC_START_REDUCE_JOB  = "Service.StartReduceJob"
	RPC_FINISH_MAP_JOB    = "Service.FinishMapJob"
	RPC_FINISH_REDUCE_JOB = "Service.FinishReduceJob"

	RPC_MAP_TRASK   = "Service.MapTask"
	RPC_REDUCE_TASK = "Service.ReduceTask"
)

type Message struct {
	Task   Task
	Finish bool
}

type Service struct {
	Info        common.Node
	Mutex       sync.Mutex
	NumExecutor int
	Tasks       []Message
	Data        map[string][]string
}

func NewService(info common.Node) *Service {
	service := new(Service)
	service.Info = info
	service.Tasks = make([]Message, 0)
	service.Data = make(map[string][]string)

	return service
}

func (service *Service) Start() {
	log.Info("Starting MapleJuice worker")
	go common.StartRPCServer(service.Info.Hostname, service.Info.MapleJuiceRPCPort, service)
}

func merge(m1 map[string][]string, m2 map[string][]string) map[string][]string {
	m := m1
	for k, v := range m2 {
		m[k] = append(m[k], v...)
	}
	return m
}

func (service *Service) allocate(size int) {
	service.Mutex.Lock()
	defer service.Mutex.Unlock()

	for i := 0; i < size; i++ {
		go service.StartExecutor()
	}

	log.Println("Allocated", size, "workers")
	service.NumExecutor += size
}

func (service *Service) AddTask(task Task) {
	service.Mutex.Lock()
	defer service.Mutex.Unlock()
	if service.NumExecutor == 0 {
		log.Fatal("No executors are available for this task:", task)
	}
	service.Tasks = append(service.Tasks, Message{Task: task, Finish: false})
}

func (service *Service) MapTask(args *MapTask, reply *bool) error {
	service.AddTask(args)
	return nil
}

func (service *Service) ReduceTask(args *ReduceTask, reply *bool) error {
	service.AddTask(args)
	return nil
}

func (service *Service) free() {
	service.Mutex.Lock()
	defer service.Mutex.Unlock()

	for i := 0; i < service.NumExecutor; i++ {
		service.Tasks = append(service.Tasks, Message{Task: nil, Finish: true})
	}

	log.Println("Deallocated", service.NumExecutor, "workers")
	service.NumExecutor = 0
}

func (server *Service) StartMapJob(job *MapJob, reply *bool) error {
	log.Println("StartMapJob()")
	sdfsClient := server.getSDFSClient()
	server.free()
	server.allocate(job.NumMapper)
	return server.downloadExecutable(sdfsClient, job.MapperExe)
}

func (server *Service) StartReduceJob(job *ReduceJob, reply *bool) error {
	log.Println("StartReduceJob()")
	sdfsClient := server.getSDFSClient()
	server.free()
	server.allocate(job.NumReducer)
	return server.downloadExecutable(sdfsClient, job.ReducerExe)
}

func (server *Service) FinishMapJob(job *MapJob, reply *bool) error {
	log.Println("FinishMapJob()")

	go func() {
		defer server.free()

		sdfsClient := server.getSDFSClient()
		server.Mutex.Lock()
		defer server.Mutex.Unlock()

		for key, values := range server.Data {
			outputFile := fmt.Sprintf("%s:%d:%s", job.OutputPrefix, server.Info.ID, key)
			lines := strings.Join(values, "\n") + "\n"
			err := sdfsClient.WriteFile(client.NewByteReader([]byte(lines)), outputFile, common.FILE_APPEND)
			if err != nil {
				log.Fatal(err)
			}
		}

		server.Data = make(map[string][]string)

		conn, err := common.Connect(common.MAPLE_JUICE_LEADER_ID, common.MAPLEJUICE_NODE)
		if err != nil {
			log.Fatal(err)
		}

		defer conn.Close()

		reply := false
		if err = conn.Call(RPC_WORKER_ACK, &server.Info.ID, &reply); err != nil {
			log.Fatal(err)
		}

	}()

	return nil
}

func (server *Service) FinishReduceJob(job *ReduceJob, reply *bool) error {
	log.Println("FinishReduceJob()")

	go func() {
		defer server.free()

		sdfsClient := server.getSDFSClient()

		server.Mutex.Lock()
		defer server.Mutex.Unlock()

		data := ""
		for key, values := range server.Data {
			for _, value := range values {
				data += key + ":" + value + "\n"
			}
		}

		if len(data) > 0 {
			reader := client.NewByteReader([]byte(data))
			outputFile := fmt.Sprintf("%s:%d", job.OutputFile, server.Info.ID)
			err := sdfsClient.WriteFile(reader, outputFile, common.FILE_APPEND)
			if err != nil {
				log.Fatal(err)
			}
		}

		server.Data = make(map[string][]string)

		conn, err := common.Connect(common.MAPLE_JUICE_LEADER_ID, common.MAPLEJUICE_NODE)
		if err != nil {
			log.Fatal(err)
		}

		defer conn.Close()

		reply := false
		if err = conn.Call(RPC_WORKER_ACK, &server.Info.ID, &reply); err != nil {
			log.Fatal(err)
		}

	}()

	return nil
}

func (server *Service) downloadExecutable(sdfsClient *client.SDFSClient, filename string) error {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	fileWriter, err := client.NewFileWriterWithOpts(filename, client.DEFAULT_FILE_FLAGS, 0777)
	if err != nil {
		log.Warn(err)
		return err
	}

	if err := sdfsClient.DownloadFile(fileWriter, filename); err != nil {
		log.Warn(err)
		return err
	}

	return nil
}

func (server *Service) StartExecutor() error {
	// Connect to leader
	conn, err := common.Connect(common.MAPLE_JUICE_LEADER_ID, common.MAPLEJUICE_NODE)
	if err != nil {
		return err
	}
	defer conn.Close()

	sdfsClient := server.getSDFSClient()
	log.Println("Executor started")

	for {
		server.Mutex.Lock()
		if len(server.Tasks) == 0 {
			server.Mutex.Unlock()
			time.Sleep(EXECUTOR_POLL_INTERVAL)
			continue
		}

		message := server.Tasks[0]
		server.Tasks = server.Tasks[1:]
		server.Mutex.Unlock()

		if message.Finish {
			log.Println("Executor finished")
			return nil
		}

		res, err := message.Task.Run(sdfsClient)
		if err != nil {
			log.Fatal(err)
		}

		if res != nil {
			server.Mutex.Lock()
			server.Data = merge(server.Data, res)
			server.Mutex.Unlock()
		}

		reply := false
		if err = conn.Call(RPC_WORKER_ACK, &server.Info.ID, &reply); err != nil {
			log.Fatal(err)
		}

	}
}

func (service *Service) getSDFSClient() *client.SDFSClient {
	return client.NewSDFSClient(common.GetAddress(service.Info.Hostname, service.Info.SDFSRPCPort))
}
