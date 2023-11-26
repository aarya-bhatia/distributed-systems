package maplejuice

import (
	"cs425/filesystem/client"
	"fmt"
	"math/rand"
	"strings"

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
	keyFiles := make(map[string][]string)

	for _, inputFile := range job.InputFiles {
		log.Println("Input File:", inputFile)
		tokens := strings.Split(inputFile, ":")
		key := tokens[len(tokens)-1]
		keyFiles[key] = append(keyFiles[key], inputFile)
	}

	for key, files := range keyFiles {
		reduceTask := &ReduceTask{
			ID:         rand.Int63(),
			Param:      job.Param,
			Key:        key,
			InputFiles: files,
		}
		res = append(res, reduceTask)
	}

	log.Printf("Total input files: %d, total reduce tasks: %d", len(job.InputFiles), len(res))
	return res, nil
}
