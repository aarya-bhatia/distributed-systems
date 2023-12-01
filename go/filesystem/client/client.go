package client

import (
	"cs425/common"
	"cs425/filesystem/server"
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"time"
	// log "github.com/sirupsen/logrus"
)

type SDFSClient struct {
	SDFSServer string
	Leader     int
}

func NewSDFSClient(SDFSServer string) *SDFSClient {
	return &SDFSClient{SDFSServer: SDFSServer}
}

func GetClientID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s.%d.%d", hostname, time.Now().UnixNano(), rand.Int())
}

func (client *SDFSClient) GetLeader() (*rpc.Client, error) {
	conn, err := rpc.Dial("tcp", client.SDFSServer)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var args bool
	reply := 1
	if err = conn.Call(server.RPC_GET_LEADER, &args, &reply); err != nil {
		return nil, err
	}

	leaderConn, err := common.Connect(reply, common.SDFS_NODE)
	if err != nil {
		return nil, err
	}

	client.Leader = reply
	// log.Debug("connect to leader:", reply)
	return leaderConn, nil
}

func (client *SDFSClient) DeleteFile(filename string) error {
	leader, err := client.GetLeader()
	if err != nil {
		return err
	}
	defer leader.Close()
	file, err := client.GetFile(filename)
	if err != nil {
		return nil
	}
	reply := true
	clientID := GetClientID()
	deleteArgs := server.DeleteArgs{ClientID: clientID, File: file.File}
	return leader.Call(server.RPC_DELETE_FILE, &deleteArgs, &reply)
}

func (client *SDFSClient) DeleteAll(prefix string) error {
	leader, err := client.GetLeader()
	if err != nil {
		return err
	}
	defer leader.Close()

	files, err := client.ListDirectory(prefix)
	if err != nil {
		return err
	}

	for _, file := range *files {
		if err := client.DeleteFile(file); err != nil {
			return err
		}
	}

	return nil
}

func (client *SDFSClient) GetFile(filename string) (*server.FileMetadata, error) {
	leader, err := client.GetLeader()
	if err != nil {
		return nil, err
	}

	defer leader.Close()

	reply := server.FileMetadata{}

	if err = leader.Call(server.RPC_GET_FILE_METADATA, &filename, &reply); err != nil {
		return nil, err
	}

	return &reply, nil
}

func (client *SDFSClient) ListDirectory(dirname string) (*[]string, error) {
	leader, err := client.GetLeader()
	if err != nil {
		return nil, err
	}

	defer leader.Close()

	reply := []string{}

	if err = leader.Call(server.RPC_LIST_DIRECTORY, &dirname, &reply); err != nil {
		return nil, err
	}

	return &reply, nil
}
