package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"errors"
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

func (job *ReduceJob) GetTasks(sdfsClient *client.SDFSClient) ([]Task, error) {
	res := []Task{}
	for _, inputFile := range job.InputFiles {
		log.Println("Input File:", inputFile)

		reduceTask := &ReduceTask{
			ID:        rand.Int63(),
			Param:     job.Param,
			InputFile: inputFile,
		}

		res = append(res, reduceTask)
	}
	return res, nil
}

func (task *ReduceTask) Run(sdfsClient *client.SDFSClient) (map[string][]string, error) {
	writer := client.NewByteWriter()
	if err := sdfsClient.DownloadFile(writer, task.InputFile); err != nil {
		log.Println(err)
		return nil, err
	}

	if err := sdfsClient.DeleteFile(task.InputFile); err != nil {
		log.Println(err)
	}

	index := strings.LastIndex(task.InputFile, "_")
	if index < 0 {
		return nil, errors.New("input file is invalid")
	}

	key := task.InputFile[index+1:]
	lines := strings.Split(writer.String(), "\n")

	// word count
	count := 0
	for _, line := range lines {
		value, err := strconv.Atoi(line)
		if err != nil {
			log.Println(err)
			continue
		}
		count += value
	}

	values := []string{fmt.Sprintf("%d", count)}
	return map[string][]string{key: values}, nil
}
