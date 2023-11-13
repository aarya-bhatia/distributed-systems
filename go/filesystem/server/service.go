package server

import (
	"cs425/common"
	"cs425/filesystem"
	"errors"
	log "github.com/sirupsen/logrus"
	"net/rpc"
	"os"
	"strings"
)

type UploadArgs struct {
	ClientID string
	Filename string
	FileSize int
}

type UploadStatus struct {
	ClientID string
	File     filesystem.File
	Blocks   []filesystem.BlockMetadata
	Success  bool
}

type UploadReply struct {
	File   filesystem.File
	Blocks []filesystem.BlockMetadata
}

type DownloadArgs struct {
	ClientID string
	Filename string
}

type DeleteArgs struct {
	ClientID string
	File     filesystem.File
}

type HeartbeatArgs struct {
	ClientID string
	Resource string
}

const (
	ErrorFileNotFound  = "ERROR File not found"
	ErrorBlockNotFound = "ERROR Block not found"
)

func (s *Server) Ping(args *bool, reply *string) error {
	*reply = "Pong"
	return nil
}

func (s *Server) Heartbeat(args *HeartbeatArgs, reply *bool) error {
	log.Debug("Heartbeat:", *args)
	return s.ResourceManager.Ping(args.ClientID, args.Resource)
}

func (s *Server) GetLeader(args *bool, reply *int) error {
	*reply = s.GetLeaderNode()
	return nil
}

func (s *Server) ListDirectory(dirname *string, reply *[]string) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	res := []string{}
	for file := range s.Files {
		// match dirname prefix with filename
		if strings.Index(file, *dirname) == 0 {
			res = append(res, file)
		}
	}
	*reply = res
	return nil
}

func (s *Server) IsFile(filename *string, reply *bool) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	_, ok := s.Files[*filename]
	*reply = ok
	return nil
}

func (s *Server) GetFileMetadata(filename *string, reply *filesystem.FileMetadata) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	file, ok := s.Files[*filename]
	if !ok {
		return errors.New(ErrorFileNotFound)
	}

	blocks := []filesystem.BlockMetadata{}
	blockSize := common.BLOCK_SIZE

	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(file.Filename, file.Version, i)
		replicas := s.BlockToNodes[blockName]
		if i == file.NumBlocks-1 {
			blockSize = file.FileSize - (file.NumBlocks-1)*common.BLOCK_SIZE
		}
		blocks = append(blocks, filesystem.BlockMetadata{Block: blockName, Replicas: replicas, Size: blockSize})
	}

	*reply = filesystem.FileMetadata{File: file, Blocks: blocks}
	return nil
}

func (s *Server) SetFileMetadata(args *filesystem.FileMetadata, reply *bool) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	if old, ok := s.Files[args.File.Filename]; ok {
		if old.Version > args.File.Version {
			return nil
		} else if old.Version < args.File.Version {
			go s.DeleteFile(&old, new(bool)) // delete old file
		}
	}

	s.Files[args.File.Filename] = args.File

	for i := 0; i < len(args.Blocks); i++ {
		block := args.Blocks[i]
		for _, replica := range block.Replicas {
			s.BlockToNodes[block.Block] = common.AddUniqueElement(s.BlockToNodes[block.Block], replica)
			s.NodesToBlocks[replica] = common.AddUniqueElement(s.NodesToBlocks[replica], block.Block)
		}
		log.Debug("block metadata was updated:", block)
	}

	log.Println("file metadata was updated:", args.File)
	return nil
}

func (s *Server) ReplicateBlocks(blocks *[]filesystem.BlockMetadata, reply *[]filesystem.BlockMetadata) error {
	connCache := filesystem.NewConnectionCache()
	defer connCache.Close()

	*reply = make([]filesystem.BlockMetadata, 0)

	for _, block := range *blocks {
		if common.FileExists(s.Directory + "/" + common.EncodeFilename(block.Block)) {
			*reply = append(*reply, block)
			continue
		}

		data, ok := filesystem.DownloadBlock(block, connCache)
		if !ok {
			continue
		}

		if common.WriteFile(s.Directory, block.Block, data, block.Size) {
			s.Mutex.Lock()
			s.BlockToNodes[block.Block] = common.AddUniqueElement(s.BlockToNodes[block.Block], s.ID)
			s.NodesToBlocks[s.ID] = common.AddUniqueElement(s.NodesToBlocks[s.ID], block.Block)
			s.Mutex.Unlock()
			*reply = append(*reply, block)
		}
	}

	log.Println("blocks replicated:", *reply)

	return nil
}

func (s *Server) FinishDownloadFile(args *DownloadArgs, reply *bool) error {
	log.Debug("Download finished:", args.Filename)
	return s.ResourceManager.Release(args.ClientID, args.Filename)
}

func (s *Server) StartDownloadFile(args *DownloadArgs, reply *filesystem.FileMetadata) error {
	log.Println("Request download:", *args)
	if err := s.ResourceManager.Acquire(args.ClientID, args.Filename, READ); err != nil {
		return err
	}

	if err := s.GetFileMetadata(&args.Filename, reply); err != nil {
		s.ResourceManager.Release(args.ClientID, args.Filename)
		return err
	}

	log.Debug("Download started:", args.Filename)
	return nil
}

func (server *Server) RequestDeleteFile(args *DeleteArgs, reply *bool) error {
	if err := server.ResourceManager.Acquire(args.ClientID, args.File.Filename, WRITE); err != nil {
		return err
	}

	defer server.ResourceManager.Release(args.ClientID, args.File.Filename)

	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	// broadcast delete request to all nodes
	for node := range server.Nodes {
		go func(node int) {
			client, err := rpc.Dial("tcp", GetAddressByID(node))
			if err != nil {
				return
			}
			defer client.Close()
			if err := client.Call("Server.DeleteFile", args.File, new(bool)); err != nil {
				log.Println(err)
			}
		}(node)
	}

	return server.DeleteFile(&args.File, reply)
}

func (server *Server) DeleteFile(file *filesystem.File, reply *bool) error {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	// delete disk blocks and metadata
	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(file.Filename, file.Version, i)
		for _, replica := range server.BlockToNodes[blockName] {
			server.NodesToBlocks[replica] = common.RemoveElement(server.NodesToBlocks[replica], blockName)
		}
		localFilename := server.Directory + "/" + common.EncodeFilename(blockName)
		if common.FileExists(localFilename) {
			os.Remove(localFilename)
		}
		delete(server.BlockToNodes, blockName)
	}

	// delete file metadata unless newer version file exists
	if found, ok := server.Files[file.Filename]; ok && found.Version <= file.Version {
		delete(server.Files, file.Filename)
	}

	log.Warn("Deleted file:", file)
	return nil
}

func (s *Server) FinishUploadFile(args *UploadStatus, reply *bool) error {
	defer s.ResourceManager.Release(args.ClientID, args.File.Filename)

	if !args.Success {
		*reply = false
		return nil
	}

	fileMetadata := &filesystem.FileMetadata{
		File:   args.File,
		Blocks: args.Blocks,
	}

	if err := s.postUpload(fileMetadata); err != nil {
		return err
	}

	log.Println("Upload finished:", args.File)
	return s.SetFileMetadata(fileMetadata, reply)
}

// update metadata at replicas synchronously
func (s *Server) postUpload(fileMetadata *filesystem.FileMetadata) error {
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

	return nil
}

func (s *Server) StartUploadFile(args *UploadArgs, reply *UploadReply) error {
	if err := s.ResourceManager.Acquire(args.ClientID, args.Filename, WRITE); err != nil {
		return err
	}

	aliveNodes := s.GetAliveNodes()

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	newFile := filesystem.File{
		Filename:  args.Filename,
		FileSize:  args.FileSize,
		Version:   1,
		NumBlocks: common.GetNumFileBlocks(int64(args.FileSize)),
	}

	if old, ok := s.Files[args.Filename]; ok {
		newFile.Version = old.Version + 1
	}

	blocks := make([]filesystem.BlockMetadata, 0)
	blockSize := common.BLOCK_SIZE

	for i := 0; i < newFile.NumBlocks; i++ {
		blockName := common.GetBlockName(newFile.Filename, newFile.Version, i)
		replicas := GetReplicaNodes(aliveNodes, blockName, common.REPLICA_FACTOR)

		if i == newFile.NumBlocks-1 {
			blockSize = newFile.FileSize - (newFile.NumBlocks-1)*common.BLOCK_SIZE
		}

		blocks = append(blocks, filesystem.BlockMetadata{
			Block:    blockName,
			Size:     blockSize,
			Replicas: replicas,
		})
	}

	reply.Blocks = blocks
	reply.File = newFile
	log.Println("Upload started:", *args)
	return nil
}
