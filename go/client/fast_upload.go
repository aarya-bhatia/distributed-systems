// TODO
package main

import (
	"container/heap"
	"cs425/common"
	"cs425/priqueue"
	"fmt"
	"net"
	"strings"
	"sync"
)

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

var pendingNodes []*Node
var completedNodes priqueue.PriorityQueue
var mutex sync.Mutex
var cond sync.Cond = *sync.NewCond(&mutex)

func FastUpload(server net.Conn, replicas []string) bool {
	pendingNodes = connectAllReplica(replicas)
	completedNodes = []*priqueue.Item{}
	heap.Init(&completedNodes)

	done := make(chan bool)
	go worker(done)

	mutex.Lock()

	for len(pendingNodes) > 0 {
		pendingNode := pendingNodes[0]
		pendingNodes = pendingNodes[1:]
		pendingNode.State = BUSY
		mutex.Unlock()
	}

	cond.Broadcast()
	mutex.Unlock()

	<-done

	Log.Info("Block upload complete: ", blockName)

	ret := []string{}

	for _, item := range completedNodes {
		node := item.Value.(*Node)
		node.Connection.Close()
		ret = append(ret, node.Address)
	}

	line := fmt.Sprintf("%s %s\n", blockName, strings.Join(ret, ","))
	if common.SendAll(server, []byte(line), len(line)) < 0 {
		return false
	}

	if len(completedNodes) < MIN_REPLICAS {
		Log.Debug("No replicas received the block")
		return false
	}

	return true
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
