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

func (client *SDFSClient) DownloadFile(localFilename string, remoteFilename string) error {
	for i := 0; i < 3; i++ {
		err := client.TryDownloadFile(localFilename, remoteFilename)
		if err == nil {
			return nil
		}
		log.Println(err)
		log.Println("Retrying download in", common.DOWNLOAD_RETRY_TIME)
		time.Sleep(common.DOWNLOAD_RETRY_TIME)
	}
	return errors.New("Max retries exceeded")
}

func (client *SDFSClient) TryDownloadFile(localFilename string, remoteFilename string) error {
	localFile, err := os.OpenFile(localFilename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	defer localFile.Close()

	connCache := filesystem.NewConnectionCache()
	defer connCache.Close()

	leader, err := client.GetLeader()
	if err != nil {
		return err
	}

	defer leader.Close()

	clientID := GetClientID()
	downloadArgs := server.DownloadArgs{ClientID: clientID, Filename: remoteFilename}
	fileMetadata := filesystem.FileMetadata{}
	if err = leader.Call(server.RPC_START_DOWNLOAD_FILE, &downloadArgs, &fileMetadata); err != nil {
		return err
	}

	reply := true
	defer leader.Call(server.RPC_FINISH_DOWNLOAD_FILE, &downloadArgs, &reply)

	h := NewHeartbeat(leader, clientID, remoteFilename, common.CLIENT_HEARTBEAT_INTERVAL)
	go h.Start()
	defer h.Stop()

	log.Println("To download:", fileMetadata.File)

	for _, block := range fileMetadata.Blocks {
		data, ok := filesystem.DownloadBlock(block, connCache)
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
