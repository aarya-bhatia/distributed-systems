package server

import (
	"cs425/common"
	log "github.com/sirupsen/logrus"
	"net/rpc"
)

type UploadArgs struct {
	Filename string
	FileSize int
}

type UploadStatus struct {
	File    File
	Blocks  []BlockMetadata
	Success bool
}

type UploadReply struct {
	File   File
	Blocks []BlockMetadata
}

func (s *Server) FinishUploadFile(args *UploadStatus, reply *bool) error {
	s.getQueue(args.File.Filename).WriteDone()

	if !args.Success {
		*reply = false
		return nil
	}

	fileMetadata := FileMetadata{
		File:   args.File,
		Blocks: args.Blocks,
	}

	// update metadata at replicas
	for _, addr := range s.GetMetadataReplicaNodes(common.REPLICA_FACTOR - 1) {
		client, err := rpc.Dial("tcp", GetAddressByID(addr))
		if err != nil {
			log.Println(err)
			return err
		}
		defer client.Close()
		reply := new(bool)
		if err = client.Call("Server.SetFileMetadata", fileMetadata, reply); err != nil {
			log.Println(err)
			return err
		}
	}

	// delete old file
	if old, ok := s.Files[args.File.Filename]; ok && old.Version < args.File.Version {
		go s.DeleteFile(&old, new(bool))
	}

	log.Println("Upload finished:", args.File)
	return s.SetFileMetadata(&fileMetadata, reply)
}

func (s *Server) StartUploadFile(args *UploadArgs, reply *UploadReply) error {
	s.getWriteLock(args.Filename)

	aliveNodes := s.GetAliveNodes()

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	newFile := File{
		Filename:  args.Filename,
		FileSize:  args.FileSize,
		Version:   1,
		NumBlocks: common.GetNumFileBlocks(int64(args.FileSize)),
	}

	if old, ok := s.Files[args.Filename]; ok {
		newFile.Version = old.Version + 1
	}

	blocks := make([]BlockMetadata, 0)
	blockSize := common.BLOCK_SIZE

	for i := 0; i < newFile.NumBlocks; i++ {
		blockName := common.GetBlockName(newFile.Filename, newFile.Version, i)
		replicas := GetReplicaNodes(aliveNodes, blockName, common.REPLICA_FACTOR)

		if i == newFile.NumBlocks-1 {
			blockSize = newFile.FileSize - (newFile.NumBlocks-1)*common.BLOCK_SIZE
		}

		blocks = append(blocks, BlockMetadata{
			Block:    blockName,
			Size:     blockSize,
			Replicas: replicas,
		})
	}

	reply.Blocks = blocks
	reply.File = newFile
	return nil
}
