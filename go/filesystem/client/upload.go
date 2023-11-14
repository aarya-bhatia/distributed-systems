package client

import (
	"cs425/common"
	"cs425/filesystem"
	"cs425/filesystem/server"
	"errors"
	log "github.com/sirupsen/logrus"
	"time"
)

func (client *SDFSClient) TryWrite(source Reader, filename string, mode int) error {
	leader, err := client.GetLeader()
	if err != nil {
		return err
	}
	defer leader.Close()

	connCache := filesystem.NewConnectionCache()
	defer connCache.Close()

	clientID := GetClientID()
	uploadArgs := server.UploadArgs{ClientID: clientID, Filename: filename, FileSize: source.Size(), Mode: mode}
	uploadReply := filesystem.FileMetadata{}
	if err = leader.Call(server.RPC_START_UPLOAD_FILE, &uploadArgs, &uploadReply); err != nil {
		return err
	}

	reply := true
	uploadStatus := server.UploadStatus{ClientID: clientID, File: uploadReply.File, Blocks: uploadReply.Blocks, Success: false}
	defer leader.Call(server.RPC_FINISH_UPLOAD_FILE, &uploadStatus, &reply)

	h := NewHeartbeat(leader, clientID, common.CLIENT_HEARTBEAT_INTERVAL)
	go h.Start()
	defer h.Stop()

	log.Println("To upload:", uploadReply.File)

	for i, block := range uploadReply.Blocks {
		freeSpace := common.BLOCK_SIZE - block.Size
		data, err := source.Read(freeSpace)
		if err != nil {
			return err
		}
		if !filesystem.UploadBlock(block, data, connCache) {
			return errors.New("failed to upload block")
		}
		uploadReply.Blocks[i].Size += len(data)
		if len(data) < freeSpace {
			break
		}
	}

	uploadStatus.Success = true
	return nil
}

func (client *SDFSClient) WriteFile(source Reader, dest string, mode int) error {
	for i := 0; i < 3; i++ {
		err := client.TryWrite(source, dest, mode)
		if err == nil {
			return nil
		}
		log.Println(err)
		log.Println("Retrying append in", common.UPLOAD_RETRY_TIME)
		time.Sleep(common.UPLOAD_RETRY_TIME)
	}

	return errors.New("Max retries were exceeded")
}
