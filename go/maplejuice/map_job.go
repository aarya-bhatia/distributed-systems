package maplejuice

import (
	"cs425/filesystem/client"
	"fmt"
	"math/rand"

	log "github.com/sirupsen/logrus"
)

const BATCH_SIZE = 200

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

func getLineGroups(lines []LineInfo, batchSize int) []LineInfo {
	batches := make([]LineInfo, 0)
	for len(lines) > 0 {
		batch := lines
		if len(lines) >= batchSize {
			batch = lines[:batchSize]
		}
		lines = lines[len(batch):]
		batchInfo := LineInfo{
			Offset: batch[0].Offset,
			Length: batch[len(batch)-1].Offset + batch[len(batch)-1].Length - batch[0].Offset,
		}
		batches = append(batches, batchInfo)
	}
	return batches
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
		lineGroups := getLineGroups(lines, BATCH_SIZE)
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

	log.Println("Created", len(res), "tasks for job", job.ID, ":", res)

	return res, nil
}
