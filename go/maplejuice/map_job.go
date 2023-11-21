package maplejuice

import (
	"cs425/filesystem/client"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
)

// A map job is a collection of map tasks
type MapJob struct {
	ID         int64
	InputFiles []string
	Param      MapParam
}

func (job *MapJob) Name() string {
	return fmt.Sprintf("<%d,maple,%s>", job.ID, job.Param.MapperExe)
}

func (job *MapJob) GetNumWorkers() int {
	return job.Param.NumMapper
}

func (job *MapJob) GetTasks(sdfsClient *client.SDFSClient) ([]Task, error) {
	res := []Task{}

	for _, inputFile := range job.InputFiles {
		log.Println("Input File:", inputFile)

		writer := client.NewByteWriter()
		if err := sdfsClient.DownloadFile(writer, inputFile); err != nil {
			return nil, err
		}

		lines := ProcessFileContents(writer.String())
		log.Println(lines)

		for _, line := range lines {
			mapTask := &MapTask{
				ID:       rand.Int63(),
				Filename: inputFile,
				Param:    job.Param,
				Offset:   line.Offset,
				Length:   line.Length,
			}

			if line.Length == 0 {
				continue
			}

			res = append(res, mapTask)
		}
	}

	log.Println("Created", len(res), "tasks for job", job.ID, ":", res)

	return res, nil
}
