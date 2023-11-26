package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strings"
)

// The parameters set by client
type ReduceParam struct {
	NumReducer  int
	ReducerExe  string
	InputPrefix string
	OutputFile  string
}

// Each reduce task is run by N reducers at a single worker node
type ReduceTask struct {
	ID        int64
	Param     ReduceParam
	InputFile string
}

func (task *ReduceTask) Hash() int {
	return common.GetHash(fmt.Sprintf("+%v", *task), common.MAX_NODES)
}

func (task *ReduceTask) GetID() int64 {
	return task.ID
}

func (task *ReduceTask) Run(sdfsClient *client.SDFSClient) (map[string][]string, error) {
	writer := client.NewByteWriter()
	if err := sdfsClient.DownloadFile(writer, task.InputFile); err != nil {
		log.Println("Error downloading input file for reduce task")
		return nil, err
	}

	lines := strings.Split(writer.String(), "\n")

	if !common.FileExists(task.Param.ReducerExe) {
		// Download reducer executable
		fileWriter, err := client.NewFileWriterWithOpts(task.Param.ReducerExe, client.DEFAULT_FILE_FLAGS, 0777)
		if err != nil {
			log.Warn("Error creating file writer")
			return nil, err
		}

		if err := sdfsClient.DownloadFile(fileWriter, task.Param.ReducerExe); err != nil {
			log.Warn("Error downloading reduce exectuable")
			return nil, err
		}
	}

	tokens := strings.Split(task.InputFile, ":")
	key := tokens[len(tokens)-1]

	output, err := ExecuteAndGetOutput("./"+task.Param.ReducerExe, lines)
	if err != nil {
		log.Warn("Error running reducer executable")
		return nil, err
	}

	return map[string][]string{key: output}, nil
}
