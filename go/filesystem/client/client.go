package client

import (
	"cs425/filesystem"
	"cs425/filesystem/server"
	"fmt"
	"net/rpc"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

type SDFSClient struct {
	SDFSServer string
}

func NewSDFSClient(SDFSServer string) *SDFSClient {
	return &SDFSClient{SDFSServer: SDFSServer}
}

func GetClientID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s.%d", hostname, time.Now().Unix())
}

func (client *SDFSClient) GetLeader() (*rpc.Client, error) {
	conn, err := rpc.Dial("tcp", client.SDFSServer)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var args bool
	reply := 1
	if err = conn.Call("Server.GetLeader", &args, &reply); err != nil {
		return nil, err
	}

	leaderConn, err := rpc.Dial("tcp", server.GetAddressByID(reply))
	if err != nil {
		return nil, err
	}

	log.Println("connect to leader:", reply)
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
	return leader.Call("Server.RequestDeleteFile", &deleteArgs, &reply)
}

func (client *SDFSClient) GetFile(filename string) (*filesystem.FileMetadata, error) {
	leader, err := client.GetLeader()
	if err != nil {
		return nil, err
	}

	defer leader.Close()

	reply := filesystem.FileMetadata{}

	if err = leader.Call("Server.GetFileMetadata", &filename, &reply); err != nil {
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

	if err = leader.Call("Server.ListDirectory", &dirname, &reply); err != nil {
		return nil, err
	}

	return &reply, nil
}
