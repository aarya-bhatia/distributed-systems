package client

import (
	"cs425/common"
	"cs425/filesystem/server"
	"errors"
	log "github.com/sirupsen/logrus"
	"time"
)

func (client *SDFSClient) WriteFile(reader Reader, dest string, mode int) error {
	for i := 0; i < common.MAX_UPLOAD_RETRIES; i++ {
		result := false
		err := client.TryWrite(reader, dest, mode, &result)
		if err == nil && result {
			return nil
		}
		log.Println(err)
		log.Println("Retrying upload in", common.UPLOAD_RETRY_TIME)
		time.Sleep(common.UPLOAD_RETRY_TIME)
	}

	return errors.New("Max retries were exceeded")
}

func (client *SDFSClient) TryWrite(reader Reader, filename string, mode int, result *bool) error {
	if err := reader.Open(); err != nil {
		log.Println(err)
		return err
	}

	defer reader.Close()

	leader, err := client.GetLeader()
	if err != nil {
		return err
	}
	defer leader.Close()

	clientID := GetClientID()

	uploadArgs := server.UploadArgs{ClientID: clientID, Filename: filename, FileSize: reader.Size(), Mode: mode}
	uploadReply := server.FileMetadata{}
	if err = leader.Call(server.RPC_START_UPLOAD_FILE, &uploadArgs, &uploadReply); err != nil {
		return err
	}

	uploadStatus := server.UploadStatus{ClientID: clientID, File: uploadReply.File, Blocks: uploadReply.Blocks, Success: false}
	defer func() {
		if err := leader.Call(server.RPC_FINISH_UPLOAD_FILE, &uploadStatus, &result); err != nil {
			log.Println(err)
		}
	}()

	h := NewHeartbeat(leader, clientID, common.CLIENT_HEARTBEAT_INTERVAL)
	go h.Start()
	defer h.Stop()

	log.Println("To upload:", uploadReply.File)

	for i, block := range uploadReply.Blocks {
		freeSpace := common.BLOCK_SIZE - block.Size
		if freeSpace == 0 {
			continue
		}

		data, err := reader.Read(freeSpace)
		if err != nil {
			return err
		}

		args := server.WriteBlockArgs{
			File: uploadReply.File,
			Block: server.Block{
				Num:  i,
				Name: block.Block,
				Data: data,
			},
			Mode: mode,
		}

		if err := client.UploadBlock(args, block.Replicas); err != nil {
			log.Println(err)
			return err
		}

		uploadReply.Blocks[i].Size += len(data)

		if len(data) < freeSpace {
			break
		}
	}

	uploadStatus.Success = true
	return nil
}

func (client *SDFSClient) UploadBlock(args server.WriteBlockArgs, replicas []int) error {
	for _, replica := range replicas {
		conn, err := common.Connect(replica)
		if err != nil {
			return err
		}
		defer conn.Close()
		log.Printf("Uploading block %v to replica %v", args.Block.Name, replica)
		reply := true
		if err := conn.Call(server.RPC_WRITE_BLOCK, &args, &reply); err != nil {
			return err
		}
	}

	return nil
}
