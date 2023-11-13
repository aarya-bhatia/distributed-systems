package client

import (
	"cs425/common"
	"cs425/filesystem/server"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

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

	clientID := GetClientID()

	uploadArgs := server.UploadArgs{ClientID: clientID, Filename: remoteFilename, FileSize: fileSize}
	uploadReply := server.UploadReply{}
	if err = leader.Call("Server.StartUploadFile", &uploadArgs, &uploadReply); err != nil {
		return err
	}

	uploadStatus := server.UploadStatus{ClientID: clientID, File: uploadReply.File, Blocks: uploadReply.Blocks, Success: false}

	stop := make(chan bool)
	go client.startHeartbeat(leader, server.HeartbeatArgs{ClientID: clientID, Resource: remoteFilename}, stop)
	defer func() {
		stop <- true
		reply := true
		leader.Call("Server.FinishUploadFile", &uploadStatus, &reply)
	}()

	log.Println("To upload:", uploadReply.File)

	if err = client.TryUploadFile(localFilename, uploadReply); err != nil {
		return err
	}

	uploadStatus.Success = true
	return nil
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
