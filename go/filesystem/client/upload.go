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
	connCache := filesystem.NewConnectionCache()
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

		if !filesystem.UploadBlock(block, data[:n], connCache) {
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

	reply := true
	uploadStatus := server.UploadStatus{ClientID: clientID, File: uploadReply.File, Blocks: uploadReply.Blocks, Success: false}
	defer leader.Call("Server.FinishUploadFile", &uploadStatus, &reply)

	h := NewHeartbeat(leader, clientID, remoteFilename, common.CLIENT_HEARTBEAT_INTERVAL)
	go h.Start()
	defer h.Stop()

	log.Println("To upload:", uploadReply.File)

	if err = client.TryUploadFile(localFilename, uploadReply); err != nil {
		return err
	}

	uploadStatus.Success = true
	return nil
}
