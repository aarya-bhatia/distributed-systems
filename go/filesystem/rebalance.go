package filesystem

import (
	"cs425/common"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"
)

func makeSet(values []string) map[string]bool {
	res := make(map[string]bool)
	for _, value := range values {
		res[value] = true
	}
	return res
}

// TODO: rebalance metadata

// To periodically redistribute file blocks to replicas to maintain equal load
func (s *Server) startRebalanceRoutine() {
	Log.Debug("Starting rebalance routine")
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

				if len(nodes) >= common.REPLICA_FACTOR {
					continue
				}

				replicas := GetReplicaNodes(aliveNodes, block, common.REPLICA_FACTOR)
				if len(replicas) == 0 {
					continue
				}

				required := makeSet(replicas)
				current := makeSet(nodes)

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
			Log.Infof("To send requests to %s: %v", replica, tasks)
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

	buffer := make([]byte, common.MIN_BUFFER_SIZE)

	for _, request := range requests {
		if strings.Index(request, "ADD_BLOCK") != 0 {
			Log.Warn("Invalid request: ", request)
			continue
		}

		n, err := conn.Write([]byte(request))
		if err != nil {
			return
		}

		n, err = conn.Read(buffer)
		if err != nil {
			return
		}

		if string(buffer)[:n-1] != "OK" {
			return
		}

		blockName := strings.Split(request, " ")[1]

		s.Mutex.Lock()

		s.BlockToNodes[blockName] = append(s.BlockToNodes[blockName], replica)
		s.NodesToBlocks[replica] = append(s.NodesToBlocks[replica], blockName)
		// Log.Debugf("Updated block metadata %s: %v", blockName, s.BlockToNodes[blockName])
		// Log.Debugf("Updated node metadata %s: %v", replica, s.NodesToBlocks[replica])
		s.Mutex.Unlock()
	}
}
