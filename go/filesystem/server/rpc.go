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

const (
	RPC_INTERNAL_SET_FILE_METADATA = "Server.InternalSetFileMetadata"
	RPC_INTERNAL_DELETE_FILE       = "Server.InternalDeleteFile"
	RPC_INTERNAL_REPLICATE_BLOCKS  = "Server.InternalReplicateBlocks"

	RPC_HEARTBEAT            = "Server.Heartbeat"
	RPC_PING                 = "Server.Ping"
	RPC_GET_FILE_METADATA    = "Server.GetFileMetadata"
	RPC_FINISH_DOWNLOAD_FILE = "Server.FinishDownloadFile"
	RPC_START_DOWNLOAD_FILE  = "Server.StartDownloadFile"
	RPC_START_UPLOAD_FILE    = "Server.StartUploadFile"
	RPC_FINISH_UPLOAD_FILE   = "Server.FinishUploadFile"
	RPC_GET_LEADER           = "Server.GetLeader"
	RPC_IS_FILE              = "Server.IsFile"
	RPC_LIST_DIRECTORY       = "Server.ListDirectory"
	RPC_DELETE_FILE          = "Server.DeleteFile"
)

// Test function
func (s *Server) Ping(args *bool, reply *string) error {
	*reply = "Pong"
	return nil
}

// Clients must call this repeatedly during upload or download, otherwise
// resource will be released
func (s *Server) Heartbeat(args *HeartbeatArgs, reply *bool) error {
	log.Debug("Heartbeat:", *args)
	return s.ResourceManager.Ping(args.ClientID, args.Resource)
}

// To get leader node ID
func (s *Server) GetLeader(args *bool, reply *int) error {
	*reply = s.GetLeaderNode()
	return nil
}

// To list files in given directory
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

// To check if file exists
func (s *Server) IsFile(filename *string, reply *bool) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	_, ok := s.Files[*filename]
	*reply = ok
	return nil
}

// To get file and blocks metadata
func (s *Server) GetFileMetadata(filename *string, reply *filesystem.FileMetadata) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	file, ok := s.Files[*filename]
	if !ok {
		return errors.New(ErrorFileNotFound)
	}

	*reply = s.Metadata.GetMetadata(file)
	return nil
}

// Clients should call this after completing download
func (s *Server) FinishDownloadFile(args *DownloadArgs, reply *bool) error {
	log.Debug("Download finished:", args.Filename)
	return s.ResourceManager.Release(args.ClientID, args.Filename)
}

// To get download access for file
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

// To request delete file
func (server *Server) DeleteFile(args *DeleteArgs, reply *bool) error {
	if err := server.ResourceManager.Acquire(args.ClientID, args.File.Filename, WRITE); err != nil {
		return err
	}

	defer server.ResourceManager.Release(args.ClientID, args.File.Filename)

	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	// broadcast delete request to all nodes
	for node := range server.Nodes {
		client, err := rpc.Dial("tcp", GetAddressByID(node))
		if err != nil {
			continue
		}
		defer client.Close()
		if err := client.Call(RPC_INTERNAL_DELETE_FILE, args.File, new(bool)); err != nil {
			log.Println(err)
		}
	}

	return server.InternalDeleteFile(&args.File, reply)
}

// To finish upload and add update metadata at replicas and self
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

	// update metadata at replicas
	for _, addr := range s.GetMetadataReplicaNodes(common.REPLICA_FACTOR - 1) {
		client, err := rpc.Dial("tcp", GetAddressByID(addr))
		if err != nil {
			log.Println(err)
			return err
		}
		defer client.Close()
		reply := new(bool)
		if err = client.Call(RPC_INTERNAL_SET_FILE_METADATA, fileMetadata, reply); err != nil {
			log.Println(err)
			return err
		}
	}

	*reply = true
	log.Println("Upload finished:", args.File)
	s.InternalSetFileMetadata(fileMetadata, new(bool))
	return nil
}

// To get upload access to file
func (s *Server) StartUploadFile(args *UploadArgs, reply *UploadReply) error {
	if err := s.ResourceManager.Acquire(args.ClientID, args.Filename, WRITE); err != nil {
		return err
	}

	aliveNodes := s.GetAliveNodes()

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	numBlocks := common.GetNumFileBlocks(int64(args.FileSize))
	newVersion := 1

	if old, ok := s.Files[args.Filename]; ok {
		newVersion = old.Version + 1
	}

	newFile := filesystem.File{
		Filename:  args.Filename,
		FileSize:  args.FileSize,
		Version:   newVersion,
		NumBlocks: numBlocks,
	}

	newMetadata := s.Metadata.GetNewMetadata(newFile, aliveNodes)

	reply.File = newMetadata.File
	reply.Blocks = newMetadata.Blocks

	log.Println("Upload started:", *args)
	return nil
}

// To update file metadata at node
func (s *Server) InternalSetFileMetadata(args *filesystem.FileMetadata, reply *bool) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	if old, ok := s.Files[args.File.Filename]; ok {
		if old.Version > args.File.Version { // given file is older than current
			return nil // do nothing
		} else if old.Version < args.File.Version { // given file is newer than current
			go s.InternalDeleteFile(&old, new(bool)) // delete old file
		}
	}

	s.Files[args.File.Filename] = args.File

	for _, block := range args.Blocks {
		s.Metadata.UpdateBlockMetadata(block)
	}

	return nil
}

// To delete given file at current node
func (server *Server) InternalDeleteFile(file *filesystem.File, reply *bool) error {
	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	// delete disk blocks and metadata
	for i := 0; i < file.NumBlocks; i++ {
		blockName := common.GetBlockName(file.Filename, file.Version, i)
		server.Metadata.RemoveBlock(blockName)
		localFilename := server.Directory + "/" + common.EncodeFilename(blockName)
		if common.FileExists(localFilename) {
			os.Remove(localFilename)
		}
	}

	// delete file metadata unless newer version file exists
	if found, ok := server.Files[file.Filename]; ok && found.Version <= file.Version {
		delete(server.Files, file.Filename)
	}

	log.Warn("Deleted file:", file)
	return nil
}

// To replicate given blocks at current node
func (s *Server) InternalReplicateBlocks(blocks *[]filesystem.BlockMetadata, reply *[]filesystem.BlockMetadata) error {
	connCache := filesystem.NewConnectionCache()
	defer connCache.Close()

	*reply = make([]filesystem.BlockMetadata, 0)

	for _, block := range *blocks {
		if common.FileExists(s.Directory + "/" + common.EncodeFilename(block.Block)) {
			*reply = append(*reply, block) // block already exists
			continue
		}

		// download block from replica
		data, ok := filesystem.DownloadBlock(block, connCache)
		if !ok {
			continue
		}

		// save block to disk
		if common.WriteFile(s.Directory, block.Block, data, block.Size) {
			s.Mutex.Lock()
			block.Replicas = append(block.Replicas, s.ID) // add self to replicas
			s.Metadata.UpdateBlockMetadata(block)
			s.Mutex.Unlock()
			*reply = append(*reply, block)

			log.Println("block replicated:", block)
		}
	}

	return nil
}
