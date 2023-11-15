package server

import (
	"cs425/common"
	"net/rpc"
	"time"

	log "github.com/sirupsen/logrus"
)

// To periodically redistribute file blocks to replicas
func (s *Server) startRebalanceRoutine() {
	log.Debug("Starting rebalance routine")
	for {
		aliveNodes := s.GetAliveNodes()
		replicaTasks := make(map[int][]BlockMetadata)

		for _, file := range s.GetFiles() {
			metadata := FileMetadata{}
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
			s.sendRebalanceRequests(replica, tasks)
		}

		time.Sleep(common.REBALANCE_INTERVAL)
	}
}
func (s *Server) sendRebalanceRequests(replica int, blocks []BlockMetadata) {
	addr := GetAddressByID(replica)
	conn, err := rpc.Dial("tcp", addr)
	if err != nil {
		return
	}
	defer conn.Close()

	reply := []BlockMetadata{}

	if err := conn.Call(RPC_INTERNAL_REPLICATE_BLOCKS, &blocks, &reply); err != nil {
		log.Println(err)
		return
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	for _, block := range reply {
		s.Metadata.UpdateBlockMetadata(block)
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
				metadata := FileMetadata{}
				s.GetFileMetadata(&file.Filename, &metadata)
				client.Call(RPC_INTERNAL_SET_FILE_METADATA, &metadata, new(bool))
			}
		}

		time.Sleep(common.REBALANCE_INTERVAL)
	}
}