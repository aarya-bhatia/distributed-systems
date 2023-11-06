package filesystem

import (
	"cs425/common"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"strings"
)

func (s *Server) replicateMetadata() {
	connections := s.getMetadataReplicaConnections()
	defer common.CloseAll(connections)
	s.Mutex.Lock()
	for _, file := range s.Files {
		for i := 0; i < file.NumBlocks; i++ {
			blockName := common.GetBlockName(file.Filename, file.Version, i)
			go replicateFileMetadata(connections, file)
			go replicateBlockMetadata(connections, blockName, s.BlockToNodes[blockName])
		}
	}
	s.Mutex.Unlock()
}

func replicateFileMetadata(connections []net.Conn, newFile File) bool {
	log.Debugf("To replicate file %s metadata to %d nodes", newFile.Filename, len(connections))
	message := fmt.Sprintf("SETFILE %s %d %d %d\n", newFile.Filename, newFile.Version, newFile.FileSize, newFile.NumBlocks)
	for _, conn := range connections {
		if !common.SendMessage(conn, message) || !common.CheckMessage(conn, "SETFILE_OK") {
			log.Debug("Failed to replicate file metadata to node ", conn.RemoteAddr())
			return false
		}
	}
	return true
}

func replicateBlockMetadata(connections []net.Conn, blockName string, replicas []string) bool {
	if len(replicas) == 0 {
		return true
	}

	log.Debugf("To replicate block %s metadata to %d nodes", blockName, len(connections))
	message := fmt.Sprintf("SETBLOCK %s %s\n", blockName, strings.Join(replicas, ","))
	for _, conn := range connections {
		if !common.SendMessage(conn, message) || !common.CheckMessage(conn, "SETBLOCK_OK") {
			log.Debug("Failed to replicate block metadata to node ", conn.RemoteAddr())
			return false
		}
	}
	return true
}
