package maplejuice

import (
	"cs425/common"
	"net"

	log "github.com/sirupsen/logrus"
)

const (
	IDLE  = 0
	MAPLE = 1 // map
	JUICE = 2 // reduce
)

const MAPLE_CHUNK_LINE_COUNT = 100

type Job struct {
	ID           int64
	Type         int
	MapperExe    string
	ReducerExe   string
	InputDir     string
	OutputPrefix string
	OutputFile   string
	InputFiles   []string
	NumMapper    int
	NumReducer   int
	Client       net.Conn
}

func (server *Leader) addJob(job *Job) {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()
	server.Jobs = append(server.Jobs, job)
	log.Info("Job enqueued:", *job)
	server.CV.Broadcast()
}

func (server *Leader) runJob(job *Job) {
	log.Println("Start Maple job", job.ID)

	defer func() {
		server.Status = IDLE
		log.Println("Finish Maple job", job.ID)
		job.Client.Close()
	}()

	var st bool

	if job.Type == MAPLE {
		st = server.runMapleJob(job)
	} else {
		st = server.runJuiceJob(job)
	}

	if st {
		common.SendMessage(job.Client, "OK")
	} else {
		common.SendMessage(job.Client, "ERROR")
	}
}

func (server *Leader) runMapleJob(job *Job) bool {
	// for inputFile := job.InputFiles
	inputFile := "data/story" // TODO
	log.Println("input file:", inputFile)
	fs := NewFileSplitter(inputFile)
	if fs == nil {
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
	return true
}

func (server *Leader) runJuiceJob(job *Job) bool {
	return false
}

func (server *Leader) runJobs() {
	for {
		server.Mutex.Lock()
		// wait for a job
		log.Println("waiting for jobs...")
		for len(server.Jobs) == 0 {
			server.CV.Wait()
		}
		// deuqueue job from front
		job := server.Jobs[0]
		server.Jobs = server.Jobs[1:]
		server.Status = job.Type
		server.CV.Broadcast()
		log.Println("job dequeued:", *job)
		go server.runJob(job)
		// wait for job to finish
		log.Println("job started.")
		for server.Status == job.Type {
			server.CV.Wait()
		}
		log.Println("job finished.")
		server.Mutex.Unlock()
	}
}
