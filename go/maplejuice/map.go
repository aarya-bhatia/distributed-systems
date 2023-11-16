package maplejuice

import (
	"cs425/common"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
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
	Task *MapTask
	Data []string
}

func (job *MapJob) Name() string {
	return fmt.Sprintf("<%d,maple,%s>", job.ID, job.Param.MapperExe)
}

func (job *MapJob) Run(server *Leader) bool {
	for _, inputFile := range job.InputFiles {
		log.Println("Input File:", inputFile)

		fs := NewFileSplitter(inputFile)
		if fs == nil {
			return false
		}

		offset := 0

		for {
			lines, ok := fs.Next(common.MAPLE_CHUNK_LINE_COUNT)
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

	return true
}

func (task *MapTask) Start(worker int, data TaskData) bool {
	lines := data.([]string)
	log.Println("Started map task:", len(lines), "lines")

	client, err := common.Connect(worker)
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
