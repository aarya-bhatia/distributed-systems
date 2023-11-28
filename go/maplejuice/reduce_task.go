package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	log "github.com/sirupsen/logrus"
)

// Each reduce task is run by N reducers at a single worker node
type ReduceTask struct {
	ID         int64
	Job        ReduceJob
	Key        string
	InputFiles []string
}

func (task *ReduceTask) Hash() int {
	return common.GetHash(fmt.Sprintf("+%v", *task), common.MAX_NODES)
}

func (task *ReduceTask) GetID() int64 {
	return task.ID
}

func (task *ReduceTask) Run(sdfsClient *client.SDFSClient) (map[string][]string, error) {
	lines := "" // Combined lines from all input files

	// All parts of the input file correspond to the same key
	for _, file := range task.InputFiles {
		writer := client.NewByteWriter()
		if err := sdfsClient.DownloadFile(writer, file); err != nil {
			log.Println("Error downloading input file for reduce task")
			return nil, err
		}

		lines += writer.String() + "\n"
	}

	args := append([]string{task.Key}, task.Job.Args...)
	output, err := ExecuteAndGetOutput("./"+task.Job.ReducerExe, args, lines)
	if err != nil {
		log.Warn("Error running reducer executable")
		return nil, err
	}

	res := ParseKeyValuePairs(output)
	return res, nil
}

func (task *ReduceTask) GetExecutable() string {
	return task.Job.ReducerExe
}
