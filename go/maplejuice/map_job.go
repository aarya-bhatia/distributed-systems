package maplejuice

import (
	"cs425/filesystem/client"
	"fmt"
	"math/rand"

	log "github.com/sirupsen/logrus"
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

		fileSize := len(writer.String())
		lines := ProcessFileContents(writer.String())
		lineGroups := GetLineGroups(lines, BATCH_SIZE)
		log.Printf("File size: %d, Total lines: %d, Total batches: %d", fileSize, len(lines), len(lineGroups))

		for _, line := range lineGroups {
			mapTask := &MapTask{
				ID:       rand.Int63(),
				Filename: inputFile,
				Param:    job.Param,
				Offset:   line.Offset,
				Length:   line.Length,
			}

			if line.Length == 0 || line.Offset+line.Length > fileSize {
				continue
			}

			res = append(res, mapTask)
		}
	}

	log.Println("Created", len(res), "tasks for job", job.ID)

	return res, nil
}
