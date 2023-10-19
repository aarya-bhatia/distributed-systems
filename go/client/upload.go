package main

import (
	"bufio"
	"container/heap"
	"cs425/common"
	"cs425/priqueue"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

const MIN_REPLICAS = 1

const (
	PENDING   = 0
	BUSY      = 1
	COMPLETED = 2
	FAILED    = 3
)

type Node struct {
	Address     string
	Connection  net.Conn
	State       int
	NumUploads  int
	UploadingTo *Node
}

var blockName string
var blockSize int
var blockData []byte = make([]byte, common.BLOCK_SIZE)

var pendingNodes []*Node
var completedNodes priqueue.PriorityQueue
var mutex sync.Mutex
var cond sync.Cond = *sync.NewCond(&mutex)

func UploadFile(localFilename string, remoteFilename string) bool {
	info, err := os.Stat(localFilename)
	if err != nil {
		Log.Warn(err)
		return false
	}

	fileSize := info.Size()
	file, err := os.Open(localFilename)
	if err != nil {
		Log.Warn(err)
		return false
	}

	Log.Debugf("Uploading file %s with %d bytes (%d blocks)", localFilename, fileSize, common.GetNumFileBlocks(int64(fileSize)))

	defer file.Close()

	message := fmt.Sprintf("UPLOAD_FILE %s %d\n", remoteFilename, fileSize)

	server := Connect()
	defer server.Close()
	// TODO: Ask for leader node

	if common.SendAll(server, []byte(message), len(message)) < 0 {
		return false
	}

	reader := bufio.NewReader(server)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			Log.Warn(err)
			break
		}

		line = line[:len(line)-1]
		Log.Debug("Received:", line)
		tokens := strings.Split(line, " ")
		if len(tokens) != 2 {
			break
		}

		replicas := strings.Split(tokens[1], ",")

		blockName = tokens[0]
		blockSize, err = file.Read(blockData)
		if err != nil {
			Log.Warn(err)
			return false
		}
		if !UploadBlockSync(server, replicas, blockData, blockSize) {
			return false
		}
	}

	server.Write([]byte("END\n"))
	return true
}

func UploadBlockSync(server net.Conn, replicas []string, fileBlock []byte, blockSize int) bool {
	for _, replica := range replicas {
		Log.Debugf("Uploading block %s (%d bytes) to %s\n", blockName, blockSize, replica)

		conn, err := net.Dial("tcp", replica)
		if err != nil {
			return false
		}

		defer conn.Close()
		Log.Debug("Connected to ", replica)

		_, err = conn.Write([]byte(fmt.Sprintf("UPLOAD %s %d\n", blockName, blockSize)))
		if err != nil {
			return false
		}

		Log.Debug("Sent upload block request to ", replica)

		if !common.GetOKMessage(conn) {
			return false
		}

		Log.Debug("Got OK from ", replica)

		if common.SendAll(conn, fileBlock[:blockSize], blockSize) < 0 {
			return false
		}

		Log.Debugf("Sent block (%d bytes) to %s\n", blockSize, replica)

		_, err = server.Write([]byte(fmt.Sprintf("%s %s\n", blockName, replica)))
		if err != nil {
			return false
		}

		Log.Debug("Sent update to server")
	}

	return true
}

func UploadBlockAsync() {
	// pendingNodes = connectAllReplica(replicas)
	// completedNodes = []*priqueue.Item{}
	// heap.Init(&completedNodes)
	//
	// done := make(chan bool)
	// go worker(done)
	//
	// mutex.Lock()
	//
	// for len(pendingNodes) > 0 {
	// 	pendingNode := pendingNodes[0]
	// 	pendingNodes = pendingNodes[1:]
	// 	pendingNode.State = BUSY
	// 	mutex.Unlock()
	// }
	//
	// cond.Broadcast()
	// mutex.Unlock()
	//
	// <-done
	//
	// Log.Info("Block upload complete: ", blockName)
	//
	// ret := []string{}
	//
	// for _, item := range completedNodes {
	// 	node := item.Value.(*Node)
	// 	node.Connection.Close()
	// 	ret = append(ret, node.Address)
	// }
	//
	// line = fmt.Sprintf("%s %s\n", blockName, strings.Join(ret, ","))
	// if common.SendAll(server, []byte(line), len(line)) < 0 {
	// 	return false
	// }
	//
	// if len(completedNodes) < MIN_REPLICAS {
	// 	Log.Debug("No replicas received the block")
	// 	return false
	// }

}

func handleFailure(node *Node) {
	mutex.Lock()
	defer mutex.Unlock()
	node.State = FAILED
	if node.UploadingTo != nil {
		pendingNodes = append(pendingNodes, node.UploadingTo)
	}
}

func startTransfer(dest *Node, src *Node) {
	request := fmt.Sprintf("ADD_BLOCK %s %d %s\n", blockName, blockSize, src.Address)
	_, err := dest.Connection.Write([]byte(request))
	if err != nil {
		handleFailure(dest)
		return
	}
	if !common.GetOKMessage(dest.Connection) {
		handleFailure(dest)
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	src.State = COMPLETED
	src.NumUploads++
	src.UploadingTo = nil

	dest.State = COMPLETED

	heap.Push(&completedNodes, &priqueue.Item{Key: src.NumUploads, Value: src})
	heap.Push(&completedNodes, &priqueue.Item{Key: dest.NumUploads, Value: dest})

	cond.Broadcast()
}

func worker(done chan bool) {
	for {
		mutex.Lock()

		if len(pendingNodes) == 0 {
			break
		}

		for len(completedNodes) == 0 {
			cond.Wait()

			if len(pendingNodes) == 0 {
				break
			}
		}

		completedNode := heap.Pop(&completedNodes).(*priqueue.Item).Value.(*Node)
		completedNode.State = BUSY

		pendingNode := pendingNodes[0]
		pendingNodes = pendingNodes[1:]
		pendingNode.State = BUSY

		completedNode.UploadingTo = pendingNode
		mutex.Unlock()

		go startTransfer(pendingNode, completedNode)
	}

	done <- true
}

func connectAllReplica(replicas []string) []*Node {
	res := []*Node{}
	for _, replica := range replicas {
		conn, err := net.Dial("tcp", replica)
		if err != nil {
			Log.Warn("Failed to connect to: ", replica)
		} else {
			Log.Info("Connected to: ", replica)
			node := new(Node)
			node.Address = replica
			node.Connection = conn
			res = append(res, node)
		}
	}
	return res
}
