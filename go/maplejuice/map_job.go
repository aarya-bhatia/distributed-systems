package maplejuice

import (
	"cs425/common"
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"
)

type MapJob struct {
	ID           int64
	NumMapper    int
	MapperExe    string
	InputFiles   []string
	InputDir     string
	OutputPrefix string
	Client       net.Conn
}

func (job *MapJob) Name() string {
	return fmt.Sprintf("<%d,maple,%s>", job.ID, job.MapperExe)
}

func (job *MapJob) Run(server *Leader) bool {
	defer func() {
		job.Client.Close()
	}()

	inputFile := "data/story" // TODO
	log.Println("input file:", inputFile)
	fs := NewFileSplitter(inputFile)
	if fs == nil {
		common.SendMessage(job.Client, "ERROR")
		return false
	}

	offset := 0
	for {
		lines, ok := fs.Next(MAPLE_CHUNK_LINE_COUNT)
		if !ok {
			break
		}

		log.Println("Read", len(lines), "lines from file")

		mapTask := new(MapTask)
		mapTask.Filename = inputFile
		mapTask.OffsetLines = offset
		mapTask.CountLines = len(lines)
		offset += len(lines)

		server.Scheduler.PutTask(mapTask, lines)
	}

	server.Scheduler.Wait()
	common.SendMessage(job.Client, "OK")
	return true
}
