package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"regexp"
	"strings"
)

// The parameters set by client
type MapParam struct {
	NumMapper    int
	MapperExe    string
	OutputPrefix string
	InputDir     string
}

// A map job is a collection of map tasks
type MapJob struct {
	ID         int64
	InputFiles []string
	Param      MapParam
}

// A map task is run by an executor on a worker node
type MapTask struct {
	ID       int64
	Param    MapParam
	Filename string
	Offset   int
	Length   int
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

			res = append(res, mapTask)

			if line.Length == 0 {
				continue
			}
		}
	}

	log.Println("Created", len(res), "tasks for job", job.ID, ":", res)

	return res, nil
}

func (task *MapTask) GetID() int64 {
	return task.ID
}

func (task *MapTask) Hash() int {
	return common.GetHash(fmt.Sprintf("+%v", *task), common.MAX_NODES)
}

// Run mapper and save output in sdfs
func (task *MapTask) Run(sdfsClient *client.SDFSClient) error {
	writer := client.NewByteWriter()
	if err := sdfsClient.ReadFile(writer, task.Filename, task.Offset, task.Length); err != nil {
		return nil
	}

	lines := strings.Split(writer.String(), "\n")
	log.Println("Running mapper with", len(lines), "lines")
	res, err := WordCountMapper(lines)
	if err != nil {
		return err
	}

	for key, value := range res {
		filename := task.Param.OutputPrefix + "_" + common.EncodeFilename(key)
		data := fmt.Sprintf("%s:%d\n", key, value)
		if err := sdfsClient.WriteFile(client.NewByteReader([]byte(data)), filename, common.FILE_APPEND); err != nil {
			return err
		}
	}

	return nil
}

func WordCountMapper(lines []string) (map[string]int, error) {
	res := make(map[string]int)
	// Define a regular expression pattern to match special characters
	reg, err := regexp.Compile("[^a-zA-Z0-9\\s]+")
	if err != nil {
		fmt.Println("Error compiling regex:", err)
		return nil, err
	}

	for _, line := range lines {
		// Remove special characters from the line
		cleanedLine := reg.ReplaceAllString(line, " ")

		// Split the cleaned line into words
		words := strings.Fields(cleanedLine)

		for _, word := range words {
			res[word]++
		}
	}

	return res, nil
}
