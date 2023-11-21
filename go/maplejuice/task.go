package maplejuice

import "cs425/filesystem/client"

type Task interface {
	Run(sdfsClient *client.SDFSClient) (map[string][]string, error)
	Hash() int
	GetID() int64
}
