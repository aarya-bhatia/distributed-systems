package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

// The parameters set by client
type ReduceParam struct {
	NumReducer  int
	ReducerExe  string
	InputPrefix string
	OutputFile  string
}

// A reduce job is a collection of reduce tasks
type ReduceJob struct {
	ID         int64
	InputFiles []string
	Param      ReduceParam
}

// Each reduce task is run by N reducers at a single worker node
type ReduceTask struct {
	ID        int64
	Param     ReduceParam
	InputFile string
}

func (task *ReduceTask) Hash() int {
	return common.GetHash(fmt.Sprintf("+%v", *task), common.MAX_NODES)
}

func (task *ReduceTask) GetID() int64 {
	return task.ID
}

func (job *ReduceJob) Name() string {
	return fmt.Sprintf("<%d,maple,%s>", job.ID, job.Param.ReducerExe)
}

func (job *ReduceJob) GetNumWorkers() int {
	return job.Param.NumReducer
}

func (job *ReduceJob) Run(server *Leader) error {
	for _, inputFile := range job.InputFiles {
		log.Println("Input File:", inputFile)

		reduceTask := &ReduceTask{
			ID:        rand.Int63(),
			Param:     job.Param,
			InputFile: inputFile,
		}
		// server.AssignTask(reduceTask)
		log.Println("Reduce task scheduled:", reduceTask)
	}
	server.Wait()
	return nil
}

func WordCountReducer(lines []string) (map[string]int, error) {
	res := make(map[string]int)

	for _, line := range lines {
		tokens := strings.Split(line, ":")
		if len(tokens) != 2 {
			continue
		}
		key := tokens[0]
		value, err := strconv.Atoi(tokens[1])
		if err != nil {
			log.Println(err)
			continue
		}
		res[key] += value
	}

	return res, nil
}

// TODO
func (task *ReduceTask) Run(sdfsClient *client.SDFSClient) error {
	return nil
}

/* func (task *ReduceTask) StartReduceExecutor(param ReduceParam, lines []string, done chan bool) {
	writer := client.NewByteWriter()
	if err := sdfsClient.DownloadFile(writer, task.InputFile); err != nil {
		log.Println(err)
		return
	}

	lines := strings.Split(writer.String(), "\n")

	done := make(chan bool)
	go service.StartReduceExecutor(task.Param, lines, done)
	log.Println("Waiting for executor...")
	status = <-done
	log.Println("Finished reduce task")
	log.Println("Running reducer with", len(lines), "lines")
	result := false
	defer func() {
		done <- result
	}()

	res, err := WordCountReducer(lines)
	if err != nil {
		log.Println(err)
		return
	}

	data := ""
	for k, v := range res {
		data += fmt.Sprintf("%s:%d\n", k, v)
	}

	if err := sdfsClient.WriteFile(client.NewByteReader([]byte(data)), param.OutputFile, common.FILE_APPEND); err != nil {
		log.Println(err)
		return
	}

	result = true
} */
