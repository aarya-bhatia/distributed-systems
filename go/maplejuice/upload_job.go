package maplejuice

import (
	"cs425/filesystem/client"
	"fmt"
	"math/rand"
)

type UploadJob struct {
	ID           int64
	Data         map[string][]string
	NumWorker    int
	OutputPrefix string
}

func NewUploadJob(data map[string][]string, numWorker int, outputPrefix string) *UploadJob {
	return &UploadJob{
		ID:           rand.Int63(),
		Data:         data,
		NumWorker:    numWorker,
		OutputPrefix: outputPrefix,
	}
}

func (job *UploadJob) Name() string {
	return fmt.Sprintf("<%d,upload>", job.ID)
}

func (job *UploadJob) GetNumWorkers() int {
	return job.NumWorker
}

func (job *UploadJob) GetTasks(sdfsClient *client.SDFSClient) ([]Task, error) {
	res := []Task{}

	for key, values := range job.Data {
		res = append(res, &UploadTask{
			ID:           rand.Int63(),
			OutputPrefix: job.OutputPrefix,
			Key:          key,
			Values:       values,
		})
	}

	return res, nil
}
