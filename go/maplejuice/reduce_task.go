package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// Each reduce task is run by N reducers at a single worker node
type ReduceTask struct {
	ID         int64
	Job        ReduceJob
	Key        string
	InputFiles []string
}

func (task *ReduceTask) RangeHash() int {
	char := strings.ToLower(task.Key)[0]

	if char >= 'a' && char < 'a'+3 {
		return 0
	}
	if char >= 'a'+3 && char < 'a'+6 {
		return 1
	}
	if char >= 'a'+6 && char < 'a'+9 {
		return 2
	}
	if char >= 'a'+9 && char < 'a'+12 {
		return 3
	}
	if char >= 'a'+12 && char < 'a'+15 {
		return 4
	}
	if char >= 'a'+15 && char < 'a'+18 {
		return 5
	}
	if char >= 'a'+18 && char < 'a'+21 {
		return 6
	}
	if char >= 'a'+21 && char < 'a'+24 {
		return 7
	}
	if char >= 'a'+24 && char < 'a'+27 {
		return 8
	}

	return 9
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

	startTime := time.Now().UnixNano()

	args := append([]string{task.Key}, task.Job.Args...)
	output, err := ExecuteAndGetOutput("./"+task.Job.ReducerExe, args, lines)
	if err != nil {
		log.Warn("Error running reducer executable")
		return nil, err
	}

	endTime := time.Now().UnixNano()
	elapsedSec := float64(endTime-startTime) * 1e-9
	log.Println("Executable ", task.Job.ReducerExe, " took ", elapsedSec, "sec")

	res := ParseKeyValuePairs(output)
	return res, nil
}

func (task *ReduceTask) GetExecutable() string {
	return task.Job.ReducerExe
}
