package filesystem

import (
	"cs425/common"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"strings"
	"time"
)

// To periodically redistribute file blocks to replicas to maintain equal load
func (s *Server) startRebalanceRoutine() {
	log.Debug("Starting rebalance routine")
	for {
		aliveNodes := s.GetAliveNodes()
		s.Mutex.Lock()
		replicaTasks := make(map[string][]string) // maps replica to list of requests to be sent

		for _, file := range s.Files {
			for i := 0; i < file.NumBlocks; i++ {
				block := common.GetBlockName(file.Filename, file.Version, i)
				nodes, ok := s.BlockToNodes[block]
				if !ok {
					continue
				}

				// TODO: Delete extra replicas
				// if len(nodes) >= common.REPLICA_FACTOR {
				// 	continue
				// }

				replicas := GetReplicaNodes(aliveNodes, block, common.REPLICA_FACTOR)
				if len(replicas) == 0 {
					continue
				}

				required := common.MakeSet(replicas)
				current := common.MakeSet(nodes)

				blockSize := common.BLOCK_SIZE
				if i == file.NumBlocks-1 && file.FileSize%common.BLOCK_SIZE != 0 {
					blockSize = file.FileSize % common.BLOCK_SIZE
				}

				for replica := range required {
					if _, ok := current[replica]; !ok {
						source := nodes[rand.Intn(len(nodes))]
						request := fmt.Sprintf("ADD_BLOCK %s %d %s\n", block, blockSize, source)
						replicaTasks[replica] = append(replicaTasks[replica], request)
					}
				}
			}
		}

		delete(replicaTasks, s.ID)

		s.Mutex.Unlock()

		for replica, tasks := range replicaTasks {
			log.Infof("Sending %d replication tasks to node %s", len(tasks), replica)
			go s.sendRebalanceRequests(replica, tasks)
		}

		time.Sleep(common.REBALANCE_INTERVAL)
	}
}

// Send all ADD_BLOCK requests to given replica over single tcp connection
// Update server block metadata on success
func (s *Server) sendRebalanceRequests(replica string, requests []string) {
	conn, err := net.Dial("tcp", replica)
	if err != nil {
		return
	}
	defer conn.Close()

	for _, request := range requests {
		if !common.SendMessage(conn, request) {
			return
		}

		if !common.GetOKMessage(conn) {
			return
		}

		var replicas []string
		blockName := strings.Split(request, " ")[1]
		s.Mutex.Lock()
		s.BlockToNodes[blockName] = common.AddUniqueElement(s.BlockToNodes[blockName], replica)
		s.NodesToBlocks[replica] = common.AddUniqueElement(s.NodesToBlocks[replica], blockName)
		replicas = s.BlockToNodes[blockName]
		s.Mutex.Unlock()

		conn := s.getMetadataReplicaConnections()
		go replicateBlockMetadata(conn, blockName, replicas)
	}
}
