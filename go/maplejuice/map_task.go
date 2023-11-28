package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"

	log "github.com/sirupsen/logrus"
)

// A map task is run by an executor on a worker node
type MapTask struct {
	ID       int64
	Job      MapJob
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

	lines := writer.String()
	args := append([]string{task.Filename}, task.Job.Args...)
	output, err := ExecuteAndGetOutput("./"+task.Job.MapperExe, args, lines)
	if err != nil {
		log.Warn("Error running map executable")
		return nil, err
	}

	return ParseKeyValuePairs(output), nil
}

func (task *MapTask) GetExecutable() string {
	return task.Job.MapperExe
}
