package client

import (
	"cs425/common"
	"cs425/filesystem/server"
	"errors"
	log "github.com/sirupsen/logrus"
	"time"
)

func (client *SDFSClient) DownloadFile(writer Writer, filename string) error {
	for i := 0; i < 3; i++ {
		err := client.TryDownloadFile(writer, filename)
		if err == nil {
			return nil
		}
		log.Println(err)
		log.Println("Retrying download in", common.DOWNLOAD_RETRY_TIME)
		time.Sleep(common.DOWNLOAD_RETRY_TIME)
	}
	return errors.New("Max retries exceeded")
}

func (client *SDFSClient) TryDownloadFile(writer Writer, filename string) error {
	if err := writer.Open(); err != nil {
		log.Println(err)
		return err
	}

	defer writer.Close()

	leader, err := client.GetLeader()
	if err != nil {
		return err
	}
	defer leader.Close()

	clientID := GetClientID()

	h := NewHeartbeat(leader, clientID, common.CLIENT_HEARTBEAT_INTERVAL)
	go h.Start()
	defer h.Stop()

	args := server.DownloadArgs{ClientID: clientID, Filename: filename}
	fileMetadata := server.FileMetadata{}
	if err := leader.Call(server.RPC_START_DOWNLOAD_FILE, &args, &fileMetadata); err != nil {
		return err
	}

	reply := true
	defer leader.Call(server.RPC_FINISH_DOWNLOAD_FILE, &args, &reply)

	log.Println("To download:", fileMetadata.File, fileMetadata.Blocks)

	for _, block := range fileMetadata.Blocks {
		if err := client.DownloadBlock(writer, block); err != nil {
			return err
		}
	}

	return nil
}

func (client *SDFSClient) DownloadBlock(writer Writer, block server.BlockMetadata) error {
	for _, replica := range common.Shuffle(block.Replicas) {
		conn, err := common.Connect(replica)
		if err != nil {
			continue
		}
		defer conn.Close()
		log.Printf("Downloading block %v from replica %v", block, replica)
		data := []byte{}
		if err := conn.Call(server.RPC_READ_BLOCK, &block, &data); err != nil {
			continue
		}
		if err := writer.Write(data); err != nil {
			return err
		}
		return nil
	}

	return errors.New("failed to download block")
}
