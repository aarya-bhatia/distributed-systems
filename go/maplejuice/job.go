package maplejuice

import (
	"cs425/filesystem/client"
)

type Job interface {
	Name() string
	GetID() int64
	GetTasks(sdfsClient *client.SDFSClient) ([]Task, error)
	GetNumWorkers() int
}
