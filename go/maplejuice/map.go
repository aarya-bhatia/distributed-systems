package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

type MapParam struct {
	NumMapper    int
	MapperExe    string
	OutputPrefix string
	InputDir     string
}

type MapJob struct {
	ID         int64
	InputFiles []string
	Param      MapParam
}

type MapTask struct {
	Param       MapParam
	Filename    string
	OffsetLines int
	CountLines  int
}

type MapArgs struct {
	SDFSServer string
	Task       *MapTask
	Data       []string
}

func (job *MapJob) Name() string {
	return fmt.Sprintf("<%d,maple,%s>", job.ID, job.Param.MapperExe)
}

func (job *MapJob) Run(server *Leader) error {
	for _, inputFile := range job.InputFiles {
		log.Println("Input File:", inputFile)

		localSDFSNode := common.SDFSCluster[0]
		sdfsClient := client.NewSDFSClient(common.GetAddress(localSDFSNode.Hostname, localSDFSNode.RPCPort))
		writer := client.NewByteWriter()
		if err := sdfsClient.DownloadFile(writer, inputFile); err != nil {
			return err
		}

		splitter := NewSplitter(writer.String())
		offset := 0

		for {
			lines, ok := splitter.Next(common.MAPLE_CHUNK_LINE_COUNT)
			if !ok {
				break
			}

			log.Println("Read", len(lines), "lines from file")

			mapTask := &MapTask{
				Filename:    inputFile,
				OffsetLines: offset,
				CountLines:  len(lines),
				Param:       job.Param,
			}

			offset += len(lines)

			server.Scheduler.PutTask(mapTask, lines)
		}

		server.Scheduler.Wait()
	}

	return nil
}

func (task *MapTask) Start(worker int, data TaskData) bool {
	lines := data.([]string)
	log.Println("Started map task:", len(lines), "lines")

	client, err := common.Connect(worker, common.MapleJuiceCluster)
	if err != nil {
		log.Println(err)
		return false
	}
	defer client.Close()

	args := &MapArgs{
		Task: task,
		Data: data.([]string),
	}

	reply := false

	if err = client.Call("Service.MapTask", args, &reply); err != nil {
		log.Println(err)
		return false
	}

	return reply
}

// TODO
func (task *MapTask) Restart(worker int) bool {
	time.Sleep(1 * time.Second)
	return true
}
