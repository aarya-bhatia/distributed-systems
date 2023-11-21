package maplejuice

import (
	"cs425/filesystem/client"
	"fmt"
	"math/rand"

	log "github.com/sirupsen/logrus"
)

// A reduce job is a collection of reduce tasks
type ReduceJob struct {
	ID         int64
	InputFiles []string
	Param      ReduceParam
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
