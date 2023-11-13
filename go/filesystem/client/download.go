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

func DownloadBlock(block server.BlockMetadata, connCache *ConnectionCache) ([]byte, bool) {
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

	clientID := GetClientID()
	downloadArgs := server.DownloadArgs{ClientID: clientID, Filename: remoteFilename}
	fileMetadata := server.FileMetadata{}
	if err = leader.Call("Server.StartDownloadFile", &downloadArgs, &fileMetadata); err != nil {
		return err
	}

	stop := make(chan bool)
	go client.startHeartbeat(leader, server.HeartbeatArgs{ClientID: clientID, Resource: remoteFilename}, stop)
	defer func() {
		stop <- true
		reply := true
		leader.Call("Server.FinishDownloadFile", &downloadArgs, &reply)
	}()

	log.Println("To download:", fileMetadata.File)

	for _, block := range fileMetadata.Blocks {
		data, ok := DownloadBlock(block, connCache)
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
