package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
)

// The parameters set by client
type MapParam struct {
	NumMapper    int
	MapperExe    string
	OutputPrefix string
	InputDir     string
}

// A map job is a collection of map tasks
type MapJob struct {
	ID         int64
	InputFiles []string
	Param      MapParam
}

// Each map task is run by N mappers at a single worker node
type MapTask struct {
	ID       int64
	Param    MapParam
	Filename string
	Offset   int
	Length   int
}

func (job *MapJob) Name() string {
	return fmt.Sprintf("<%d,maple,%s>", job.ID, job.Param.MapperExe)
}

func (job *MapJob) Run(server *Leader) error {
	for _, inputFile := range job.InputFiles {
		log.Println("Input File:", inputFile)

		sdfsNode := common.RandomChoice(server.GetSDFSNodes())
		sdfsClient := client.NewSDFSClient(common.GetAddress(sdfsNode.Hostname, sdfsNode.RPCPort))

		writer := client.NewByteWriter()

		if err := sdfsClient.DownloadFile(writer, inputFile); err != nil {
			return err
		}

		lines := ProcessFileContents(writer.String())
		log.Println(lines)

		for _, line := range lines {
			mapTask := &MapTask{
				ID:       rand.Int63(),
				Filename: inputFile,
				Param:    job.Param,
				Offset:   line.Offset,
				Length:   line.Length,
			}

			if line.Length == 0 {
				continue
			}

			server.Scheduler.AssignTask(mapTask)
			log.Println("Map task scheduled:", mapTask)
		}

		server.Scheduler.Wait()
	}

	return nil
}

func (task *MapTask) GetID() int64 {
	return task.ID
}

func (task *MapTask) Hash() int {
	return common.GetHash(fmt.Sprintf("+%v", *task), common.MAX_NODES)
}

func (task *MapTask) Start(worker int) bool {
	client, err := common.Connect(worker, common.MapleJuiceCluster)
	if err != nil {
		log.Println(err)
		return false
	}
	defer client.Close()

	reply := false
	if err = client.Call(RPC_MAP_TASK, task, &reply); err != nil {
		log.Println(err)
		return false
	}

	return reply
}
