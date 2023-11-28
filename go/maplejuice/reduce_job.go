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
	ID          int64
	NumReducer  int
	ReducerExe  string
	InputPrefix string
	OutputFile  string
	Args        []string
}

func (job *ReduceJob) GetID() int64 {
	return job.ID
}

func (job *ReduceJob) Name() string {
	return fmt.Sprintf("<%d,maple,%s>", job.ID, job.ReducerExe)
}

func (job *ReduceJob) GetNumWorkers() int {
	return job.NumReducer
}

// TODO: CLEANUP invalid input files
func (job *ReduceJob) GetTasks(sdfsClient *client.SDFSClient) ([]Task, error) {
	res := []Task{}
	keyFiles := make(map[string][]string)

	inputFiles, err := sdfsClient.ListDirectory(job.InputPrefix)
	if err != nil {
		return nil, err
	}

	for _, inputFile := range *inputFiles {
		log.Println("Input File:", inputFile)
		tokens := strings.Split(inputFile, ":")
		key := tokens[len(tokens)-1]
		keyFiles[key] = append(keyFiles[key], inputFile)
	}

	for key, files := range keyFiles {
		reduceTask := &ReduceTask{
			ID:         rand.Int63(),
			Job:        *job,
			Key:        key,
			InputFiles: files,
		}
		res = append(res, reduceTask)
	}

	log.Printf("Total input files: %d, total reduce tasks: %d", len(*inputFiles), len(res))
	return res, nil
}
