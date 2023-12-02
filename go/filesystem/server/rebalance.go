package server

import (
	"cs425/common"
	log "github.com/sirupsen/logrus"
	"time"
)

// To periodically redistribute file blocks to replicas
func (s *Server) startRebalanceRoutine() {
	log.Debug("Starting rebalance routine")
	for {
		s.rebalance()
		time.Sleep(common.REBALANCE_INTERVAL)
	}
}

func (s *Server) rebalance() {
	if s.ID != s.GetLeaderNode() {
		return
	}

	aliveNodes := s.GetAliveNodes()
	replicaTasks := make(map[int][]BlockMetadata)

	for _, file := range s.GetFiles() {
		metadata := FileMetadata{}
		s.GetFileMetadata(&file.Filename, &metadata)
		// TODO: Delete extra replicas

		for _, block := range metadata.Blocks {
			expectedReplicas := GetReplicaNodes(aliveNodes, block.Block, common.REPLICA_FACTOR)
			// log.Printf("Block %s: current replicas: %v, expected replicas: %v", block.Block, block.Replicas, expectedReplicas)
			// get the replicas missing this block
			pendingReplicas := common.Subtract(expectedReplicas, block.Replicas)
			for _, replica := range pendingReplicas {
				replicaTasks[replica] = append(replicaTasks[replica], block)
			}
		}
	}

	// delete(replicaTasks, s.ID)

	for replica, tasks := range replicaTasks {
		log.Infof("To replicate %d blocks at node %d", len(tasks), replica)
		s.sendRebalanceRequests(replica, tasks)
	}
}

func (s *Server) sendRebalanceRequests(replica int, blocks []BlockMetadata) {
	conn, err := common.Connect(replica, common.SDFS_NODE)
	if err != nil {
		return
	}
	defer conn.Close()

	reply := []BlockMetadata{}

	if err := conn.Call(RPC_INTERNAL_REPLICATE_BLOCKS, &blocks, &reply); err != nil {
		log.Println(err)
		return
	}

	aliveNodes := s.GetAliveNodes()

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	for _, block := range reply {
		block.Replicas = common.Intersect(block.Replicas, aliveNodes)
		log.Printf("New replicas for block %s: %v", block.Block, block.Replicas)
		s.Metadata.UpdateBlockMetadata(block)
	}
}

func (s *Server) startMetadataRebalanceRoutine() {
	log.Println("Starting metadata rebalance routine")
	for {
		s.broadcastMetadata()
		time.Sleep(common.METADATA_REBALANCE_INTERVAL)
	}
}

func (s *Server) broadcastMetadata() {
	leader := s.GetLeaderNode()
	if s.ID != leader {
		s.sendMetadata(leader)
		return
	}

	for _, replica := range s.GetMetadataReplicaNodes(common.REPLICA_FACTOR - 1) {
		s.sendMetadata(replica)
	}
}

func (s *Server) sendMetadata(node int) {
	client, err := common.Connect(node, common.SDFS_NODE)
	if err != nil {
		return
	}

	defer client.Close()

	for _, file := range s.GetFiles() {
		metadata := FileMetadata{}
		s.GetFileMetadata(&file.Filename, &metadata)
		err = client.Call(RPC_INTERNAL_SET_FILE_METADATA, &metadata, new(bool))
		if err != nil {
			return
		}
	}

	// log.Debug("sent metadata to ", node)
}
