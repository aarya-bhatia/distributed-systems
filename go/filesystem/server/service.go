package server

import (
	"cs425/common"
	"errors"
	"net/rpc"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

type BlockMetadata struct {
	Block    string
	Size     int
	Replicas []int
}

type FileMetadata struct {
	File   File
	Blocks []BlockMetadata
}

const (
	ErrorFileNotFound  = "ERROR File not found"
	ErrorBlockNotFound = "ERROR Block not found"
)

func (s *Server) Ping(args *bool, reply *string) error {
	*reply = "Pong"
	return nil
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

func (s *Server) GetFileMetadata(filename *string, reply *FileMetadata) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	file, ok := s.Files[*filename]
	if !ok {
		return errors.New(ErrorFileNotFound)
	}

	blocks := []BlockMetadata{}
	blockSize := common.BLOCK_SIZE

	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(file.Filename, file.Version, i)
		replicas := s.BlockToNodes[blockName]
		if i == file.NumBlocks-1 {
			blockSize = file.FileSize - (file.NumBlocks-1)*common.BLOCK_SIZE
		}
		blocks = append(blocks, BlockMetadata{Block: blockName, Replicas: replicas, Size: blockSize})
	}

	*reply = FileMetadata{File: file, Blocks: blocks}
	return nil
}

func (s *Server) SetFileMetadata(args *FileMetadata, reply *bool) error {
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
		block := args.Blocks[i].Block
		for _, replica := range args.Blocks[i].Replicas {
			s.BlockToNodes[block] = common.AddUniqueElement(s.BlockToNodes[block], replica)
			s.NodesToBlocks[replica] = common.AddUniqueElement(s.NodesToBlocks[replica], block)
		}
	}

	log.Println("file metadata was updated:", args.File)

	return nil
}

func (s *Server) ReplicateBlock(block *BlockMetadata, reply *bool) error {
	if common.FileExists(s.Directory + "/" + common.EncodeFilename(block.Block)) {
		*reply = true
		return nil
	}

	source := common.RandomChoice(block.Replicas)
	if !replicateBlock(s.Directory, block.Block, block.Size, source) {
		*reply = false
		return nil
	}

	s.Mutex.Lock()
	s.BlockToNodes[block.Block] = common.AddUniqueElement(s.BlockToNodes[block.Block], s.ID)
	s.NodesToBlocks[s.ID] = common.AddUniqueElement(s.NodesToBlocks[s.ID], block.Block)
	s.Mutex.Unlock()

	*reply = true
	return nil
}

func (s *Server) FinishDownloadFile(filename *string, reply *bool) error {
	s.getQueue(*filename).ReadDone()
	log.Debug("Download finished:", *filename)
	return nil
}

func (s *Server) StartDownloadFile(filename *string, reply *FileMetadata) error {
	s.getReadLock(*filename)

	if err := s.GetFileMetadata(filename, reply); err != nil {
		s.getQueue(*filename).ReadDone()
		return err
	}

	log.Debug("Download started:", *filename)
	return nil
}

func (server *Server) RequestDeleteFile(file *File, reply *bool) error {
	server.getWriteLock(file.Filename)
	defer server.getQueue(file.Filename).WriteDone()

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
			if err := client.Call("Server.DeleteFile", file, new(bool)); err != nil {
				log.Println(err)
			}
		}(node)
	}

	return server.DeleteFile(file, reply)
}

func (server *Server) DeleteFile(file *File, reply *bool) error {
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
