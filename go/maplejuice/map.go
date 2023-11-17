package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	log "github.com/sirupsen/logrus"
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

		splitter := NewSplitter(writer.String())
		offset := 0

		for {
			lines, ok := splitter.Next(1)
			if !ok || len(lines) == 0 {
				break
			}

			length := 0

			for _, line := range lines {
				length += len(line) + 1
			}

			mapTask := &MapTask{
				Filename: inputFile,
				Param:    job.Param,
				Offset:   offset,
				Length:   length,
			}

			server.Scheduler.PutTask(mapTask)

			log.Println("Map task scheduled:", mapTask) //, writer.String()[offset:offset+length])
			log.Printf("offset:%d,length:%d", offset, length)

			offset += length
		}

		server.Scheduler.Wait()
	}

	return nil
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
