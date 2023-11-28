package maplejuice

import (
	"cs425/filesystem/client"
	"fmt"
	"math/rand"

	log "github.com/sirupsen/logrus"
)

// A map job is a collection of map tasks
type MapJob struct {
	ID           int64
	NumMapper    int
	MapperExe    string
	OutputPrefix string
	InputDir     string
	Args         []string
}

func (job *MapJob) GetID() int64 {
	return job.ID
}

func (job *MapJob) Name() string {
	return fmt.Sprintf("<%d,maple,%s>", job.ID, job.MapperExe)
}

func (job *MapJob) GetNumWorkers() int {
	return job.NumMapper
}

func (job *MapJob) GetTasks(sdfsClient *client.SDFSClient) ([]Task, error) {
	res := []Task{}

	inputFiles, err := sdfsClient.ListDirectory(job.InputDir)
	if err != nil {
		return nil, err
	}

	for _, inputFile := range *inputFiles {
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
				Job:      *job,
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
