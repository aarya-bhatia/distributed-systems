package client

import (
	"cs425/common"
	"cs425/filesystem/server"
	"errors"
	log "github.com/sirupsen/logrus"
	"time"
)

func (client *SDFSClient) DownloadFile(writer Writer, filename string) error {
	metadata, err := client.GetFile(filename)
	if err != nil {
		return err
	}

	return client.ReadFile(writer, filename, 0, metadata.File.FileSize)
}

func (client *SDFSClient) ReadFile(writer Writer, filename string, offset int, length int) error {
	if offset < 0 || length == 0 {
		return errors.New("Invalid offset or length")
	}

	for i := 0; i < common.MAX_DOWNLOAD_RETRIES; i++ {
		err := client.TryReadFile(writer, filename, offset, length)
		if err == nil {
			return nil
		}
		log.Println(err)
		log.Println("Retrying download in", common.DOWNLOAD_RETRY_TIME)
		time.Sleep(common.DOWNLOAD_RETRY_TIME)
	}
	return errors.New("Max retries exceeded")
}

func (client *SDFSClient) TryReadFile(writer Writer, filename string, offset int, length int) error {
	if err := writer.Open(); err != nil {
		log.Println(err)
		return err
	}

	defer writer.Close()

	pool := common.NewConnectionPool(common.SDFS_NODE)
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

	if offset+length > fileMetadata.File.FileSize {
		log.Warnf("offset:%d,length:%d,filesize:%d", offset, length, fileMetadata.File.FileSize)
		return errors.New("out of bounds")
	}

	log.Println("To download:", fileMetadata.File, fileMetadata.Blocks)

	startBlock := int(offset / common.BLOCK_SIZE)
	endBlock := int((offset + length - 1) / common.BLOCK_SIZE)
	initialOffset := offset % common.BLOCK_SIZE
	remaining := length

	// log.Debugf("To read file %s from byte %d to byte %d", filename, offset, offset+length)
	// log.Debugf("start block:%d, end block:%d, initialOffset:%d, remaining:%d", startBlock, endBlock, initialOffset, remaining)

	if endBlock >= fileMetadata.File.NumBlocks {
		return errors.New("End block is out of bounds")
	}

	for i := startBlock; i <= endBlock; i++ {
		block := fileMetadata.Blocks[i]
		toRead := common.Min(block.Size-initialOffset, remaining)

		if err := client.ReadBlock(writer, block, initialOffset, toRead, pool); err != nil {
			return err
		}

		remaining -= toRead
		initialOffset = 0
	}

	return nil
}

func (client *SDFSClient) ReadBlock(writer Writer, block server.BlockMetadata, offset int, length int, pool *common.ConnectionPool) error {
	for _, replica := range common.Shuffle(block.Replicas) {
		conn, err := pool.GetConnection(replica)
		if err != nil {
			log.Println(err)
			continue
		}
		// log.Printf("Downloading block %v from replica %v", block, replica)
		data := server.Block{}
		args := server.DownloadBlockArgs{Block: block.Block, Offset: offset, Size: length}
		if err := conn.Call(server.RPC_READ_BLOCK, &args, &data); err != nil {
			log.Println(err)
			continue
		}
		if data.Name != block.Block {
			log.Warn("Incorrect block received")
			continue
		}
		if len(data.Data) != length {
			log.Warn("Incorrect size received")
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
