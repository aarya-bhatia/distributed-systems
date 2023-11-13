package server

import (
	"cs425/common"
	"cs425/filesystem"
	log "github.com/sirupsen/logrus"
	"net/rpc"
	"time"
)

// To periodically redistribute file blocks to replicas
func (s *Server) startRebalanceRoutine() {
	log.Debug("Starting rebalance routine")
	for {
		aliveNodes := s.GetAliveNodes()
		replicaTasks := make(map[int][]filesystem.BlockMetadata)

		for _, file := range s.GetFiles() {
			metadata := filesystem.FileMetadata{}
			s.GetFileMetadata(&file.Filename, &metadata)
			// TODO: Delete extra replicas

			for _, block := range metadata.Blocks {
				current := common.MakeSet(block.Replicas)
				// get the replicas missing this block
				for _, replica := range GetReplicaNodes(aliveNodes, block.Block, common.REPLICA_FACTOR) {
					if _, ok := current[replica]; !ok {
						replicaTasks[replica] = append(replicaTasks[replica], block)
					}
				}
			}
		}

		delete(replicaTasks, s.ID)

		for replica, tasks := range replicaTasks {
			log.Infof("Sending %d replication tasks to node %d:%v", len(tasks), replica, tasks)
			go s.sendRebalanceRequests(replica, tasks)
		}

		time.Sleep(common.REBALANCE_INTERVAL)
	}
}
func (s *Server) sendRebalanceRequests(replica int, blocks []filesystem.BlockMetadata) {
	addr := GetAddressByID(replica)
	conn, err := rpc.Dial("tcp", addr)
	if err != nil {
		return
	}
	defer conn.Close()

	reply := []filesystem.BlockMetadata{}

	if err := conn.Call("Server.ReplicateBlocks", &blocks, &reply); err != nil {
		log.Println(err)
		return
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	for _, block := range reply {
		s.BlockToNodes[block.Block] = common.AddUniqueElement(s.BlockToNodes[block.Block], replica)
		s.NodesToBlocks[replica] = common.AddUniqueElement(s.NodesToBlocks[replica], block.Block)
	}
}

func (s *Server) startMetadataRebalanceRoutine() {
	log.Println("Starting metadata rebalance routine")
	for {
		for _, replica := range s.GetMetadataReplicaNodes(common.REPLICA_FACTOR - 1) {
			client, err := rpc.Dial("tcp", GetAddressByID(replica))
			if err != nil {
				continue
			}
			defer client.Close()

			for _, file := range s.GetFiles() {
				metadata := filesystem.FileMetadata{}
				s.GetFileMetadata(&file.Filename, &metadata)
				client.Call("Server.SetFileMetadata", &metadata, new(bool))
			}
		}

		time.Sleep(common.REBALANCE_INTERVAL)
	}
}
