package client

import (
	"cs425/common"
	"cs425/filesystem/server"
	"errors"
	log "github.com/sirupsen/logrus"
	"time"
)

func (client *SDFSClient) DownloadFile(writer Writer, filename string) error {
	for i := 0; i < common.MAX_DOWNLOAD_RETRIES; i++ {
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

	pool := common.NewConnectionPool()
	defer pool.Close()

	leader, err := client.GetLeader()
	if err != nil {
		return err
	}
	defer leader.Close()

	clientID := GetClientID()

	h, err := NewHeartbeat(client.Leader, clientID, common.CLIENT_HEARTBEAT_INTERVAL)
	if err != nil {
		return err
	}

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
		if err := client.DownloadBlock(writer, block, pool); err != nil {
			return err
		}
	}

	return nil
}

func (client *SDFSClient) DownloadBlock(writer Writer, block server.BlockMetadata, pool *common.ConnectionPool) error {
	for _, replica := range common.Shuffle(block.Replicas) {
		conn, err := pool.GetConnection(replica)
		if err != nil {
			log.Println(err)
			continue
		}
		log.Printf("Downloading block %v from replica %v", block, replica)
		data := server.Block{}
		if err := conn.Call(server.RPC_READ_BLOCK, &block, &data); err != nil {
			log.Println(err)
			continue
		}
		if data.Name != block.Block {
			log.Warn("Incorrect block received")
			continue
		}
		if err := writer.Write(data.Data); err != nil {
			log.Println(err)
			return err
		}
		return nil
	}

	return errors.New("failed to download block")
}
