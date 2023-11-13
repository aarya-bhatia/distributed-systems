package client

import (
	"cs425/common"
	"cs425/filesystem"
	"cs425/filesystem/server"
	"errors"
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
		if err = client.TryUploadFile(localFilename, remoteFilename); err == nil {
			return nil
		}
		log.Println(err)
		log.Println("Retrying upload in", common.UPLOAD_RETRY_TIME)
		time.Sleep(common.UPLOAD_RETRY_TIME)
	}

	return errors.New("Max retries were exceeded")
}

func (client *SDFSClient) TryUploadFile(localFilename string, remoteFilename string) error {
	stat, err := os.Stat(localFilename)
	if err != nil {
		return err
	}
	fileSize := int(stat.Size())

	localFile, err := os.Open(localFilename)
	if err != nil {
		return nil
	}
	defer localFile.Close()

	leader, err := client.GetLeader()
	if err != nil {
		return err
	}
	defer leader.Close()

	connCache := filesystem.NewConnectionCache()
	defer connCache.Close()

	clientID := GetClientID()
	uploadArgs := server.UploadArgs{ClientID: clientID, Filename: remoteFilename, FileSize: fileSize}
	uploadReply := server.UploadReply{}
	if err = leader.Call("Server.StartUploadFile", &uploadArgs, &uploadReply); err != nil {
		return err
	}

	reply := true
	uploadStatus := server.UploadStatus{ClientID: clientID, File: uploadReply.File, Blocks: uploadReply.Blocks, Success: false}
	defer leader.Call("Server.FinishUploadFile", &uploadStatus, &reply)

	h := NewHeartbeat(leader, clientID, remoteFilename, common.CLIENT_HEARTBEAT_INTERVAL)
	go h.Start()
	defer h.Stop()

	log.Println("To upload:", uploadReply.File)

	data := make([]byte, common.BLOCK_SIZE)

	for _, block := range uploadReply.Blocks {
		n, err := localFile.Read(data)
		if err != nil {
			return err
		}

		if !filesystem.UploadBlock(block, data[:n], connCache) {
			return errors.New("failed to upload block")
		}

		log.Printf("Upload block %s (%d bytes) to %d replicas: %v", block.Block, block.Size, len(block.Replicas), block.Replicas)
	}

	uploadStatus.Success = true
	return nil
}
