package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

// The parameters set by client
type MapParam struct {
	NumMapper    int
	MapperExe    string
	OutputPrefix string
	InputDir     string
}

// A map task is run by an executor on a worker node
type MapTask struct {
	ID       int64
	Param    MapParam
	Filename string
	Offset   int
	Length   int
}

func (task *MapTask) GetID() int64 {
	return task.ID
}

func (task *MapTask) Hash() int {
	return common.GetHash(fmt.Sprintf("+%v", *task), common.MAX_NODES)
}

func (task *MapTask) Run(sdfsClient *client.SDFSClient) (map[string][]string, error) {
	// Download input file chunk
	writer := client.NewByteWriter()
	if err := sdfsClient.ReadFile(writer, task.Filename, task.Offset, task.Length); err != nil {
		log.Warn("Error downloading input file for map task")
		return nil, err
	}

	lines := strings.Split(writer.String(), "\n")

	if err := CleanInputLines(lines); err != nil {
		return nil, err
	}

	if !common.FileExists(task.Param.MapperExe) {
		// Download mapper executable
		fileWriter, err := client.NewFileWriterWithOpts(task.Param.MapperExe, 0777)
		if err != nil {
			log.Warn("Error creating file writer")
			return nil, err
		}

		if err := sdfsClient.DownloadFile(fileWriter, task.Param.MapperExe); err != nil {
			log.Warn("Error downloading map exectuable")
			return nil, err
		}
	}

	// Execute executable process and parse output as key-value map
	return ExecuteAndGetOutput("./"+task.Param.MapperExe, lines)
}
