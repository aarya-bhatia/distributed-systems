package maplejuice

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

type UploadTask struct {
	ID           int64
	OutputPrefix string
	Key          string
	Values       []string
}

func (task *UploadTask) Hash() int {
	return common.GetHash(fmt.Sprintf("+%v", *task), common.MAX_NODES)
}

func (task *UploadTask) GetID() int64 {
	return task.ID
}

func (task *UploadTask) Run(sdfsClient *client.SDFSClient) (map[string][]string, error) {
	filename := task.OutputPrefix + "_" + task.Key
	if err := sdfsClient.WriteFile(client.NewByteReader([]byte(strings.Join(task.Values, "\n"))), filename, common.FILE_TRUNCATE); err != nil {
		log.Println(err)
		return nil, err
	}

	return nil, nil
}
