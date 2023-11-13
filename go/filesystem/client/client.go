package client

import (
	"cs425/common"
	"cs425/filesystem/server"
	"errors"
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
	var reply bool
	return leader.Call("Server.RequestDeleteFile", &filename, &reply)
}

func (client *SDFSClient) GetFile(filename string) (*server.FileMetadata, error) {
	leader, err := client.GetLeader()
	if err != nil {
		return nil, err
	}

	defer leader.Close()

	reply := server.FileMetadata{}

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

	if err = leader.Call("Server.GetFileMetadata", &dirname, &reply); err != nil {
		return nil, err
	}

	return &reply, nil
}

func (client *SDFSClient) DownloadFile(localFilename string, remoteFilename string) error {
	var err error
	for i := 0; i < 3; i++ {
		if err = client.TryDownloadFile(localFilename, remoteFilename); err == nil {
			return nil
		}
		log.Println(err)
		log.Println("Retrying download in", common.DOWNLOAD_RETRY_TIME)
		time.Sleep(common.DOWNLOAD_RETRY_TIME)
	}
	return errors.New("Max retries exceeded")
}

func (client *SDFSClient) DownloadBlock(block server.BlockMetadata, connCache *ConnectionCache) ([]byte, bool) {
	for _, replica := range common.Shuffle(block.Replicas) {
		if data, ok := TryDownloadBlock(block, replica, connCache); ok {
			return data, true
		}
	}
	return nil, false
}

func (client *SDFSClient) TryDownloadFile(localFilename string, remoteFilename string) error {
	localFile, err := os.OpenFile(localFilename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	defer localFile.Close()

	connCache := NewConnectionCache()
	defer connCache.Close()

	leader, err := client.GetLeader()
	if err != nil {
		return err
	}

	defer leader.Close()

	fileMetadata := server.FileMetadata{}
	if err = leader.Call("Server.StartDownloadFile", &remoteFilename, &fileMetadata); err != nil {
		return err
	}

	log.Println("To download:", fileMetadata.File)

	reply := true
	defer leader.Call("Server.FinishDownloadFile", &remoteFilename, &reply)

	for _, block := range fileMetadata.Blocks {
		data, ok := client.DownloadBlock(block, connCache)
		if !ok {
			return errors.New("failed to download block")
		}
		_, err := localFile.Write(data)
		if err != nil {
			return err
		}
	}

	return nil

}

func TryDownloadBlock(block server.BlockMetadata, replica int, connCache *ConnectionCache) ([]byte, bool) {
	node := common.GetNodeByID(replica)
	addr := common.GetAddress(node.Hostname, node.TCPPort)
	conn := connCache.GetConnection(addr)
	if conn == nil {
		return nil, false
	}
	request := fmt.Sprintf("DOWNLOAD %s\n", block.Block)
	log.Debug(request)
	if !common.SendMessage(conn, request) {
		return nil, false
	}

	buffer := make([]byte, block.Size)
	bufferSize := 0

	for bufferSize < block.Size {
		n, err := conn.Read(buffer[bufferSize:])
		if err != nil {
			log.Warn(err)
			return nil, false
		}
		bufferSize += n
	}

	if bufferSize < block.Size {
		log.Warn("Insufficient bytes:", block.Block)
		return nil, false
	}

	log.Debugf("downloaded block %s with %d bytes from %s\n", block.Block, bufferSize, addr)

	return buffer[:bufferSize], true
}

func (client *SDFSClient) UploadFile(localFilename string, remoteFilename string) error {
	if !common.FileExists(localFilename) {
		return errors.New("File not found")
	}

	var err error
	for i := 0; i < 3; i++ {
		if err = client.RequestUpload(localFilename, remoteFilename); err == nil {
			return nil
		}
		log.Println(err)
		log.Println("Retrying upload in", common.UPLOAD_RETRY_TIME)
		time.Sleep(common.UPLOAD_RETRY_TIME)
	}

	return errors.New("Max retries were exceeded")
}

func (client *SDFSClient) TryUploadFile(localFilename string, uploadReply server.UploadReply) error {
	connCache := NewConnectionCache()
	defer connCache.Close()

	localFile, err := os.Open(localFilename)
	if err != nil {
		return nil
	}

	data := make([]byte, common.BLOCK_SIZE)

	for _, block := range uploadReply.Blocks {
		n, err := localFile.Read(data)
		if err != nil {
			return err
		}

		if !client.UploadBlock(block, data[:n], connCache) {
			return errors.New("failed to upload block")
		}
	}

	return nil
}

func (client *SDFSClient) RequestUpload(localFilename string, remoteFilename string) error {
	stat, err := os.Stat(localFilename)
	if err != nil {
		return err
	}

	fileSize := int(stat.Size())

	leader, err := client.GetLeader()
	if err != nil {
		return err
	}

	defer leader.Close()

	uploadArgs := server.UploadArgs{Filename: remoteFilename, FileSize: fileSize}
	uploadReply := server.UploadReply{}
	if err = leader.Call("Server.StartUploadFile", &uploadArgs, &uploadReply); err != nil {
		return err
	}

	log.Println("To upload:", uploadReply.File)
	reply := true

	if err = client.TryUploadFile(localFilename, uploadReply); err != nil {
		uploadStatus := server.UploadStatus{File: uploadReply.File, Blocks: uploadReply.Blocks, Success: false}
		leader.Call("Server.FinishUploadFile", &uploadStatus, &reply)
		return err
	} else {
		uploadStatus := server.UploadStatus{File: uploadReply.File, Blocks: uploadReply.Blocks, Success: true}
		return leader.Call("Server.FinishUploadFile", &uploadStatus, &reply)
	}
}

func (client *SDFSClient) UploadBlock(block server.BlockMetadata, data []byte, connCache *ConnectionCache) bool {
	for _, replica := range block.Replicas {
		node := common.GetNodeByID(replica)
		addr := common.GetAddress(node.Hostname, node.TCPPort)

		log.Debugf("Uploading block %s (%d bytes) to %s\n", block.Block, block.Size, addr)

		conn := connCache.GetConnection(addr)
		if conn == nil {
			return false
		}

		if !common.SendMessage(conn, fmt.Sprintf("UPLOAD %s %d\n", block.Block, block.Size)) {
			return false
		}

		if !common.GetOKMessage(conn) {
			return false
		}

		if common.SendAll(conn, data, block.Size) < 0 {
			return false
		}

		if !common.GetOKMessage(conn) {
			return false
		}
	}

	return true
}
