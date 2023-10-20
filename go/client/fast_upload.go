<<<<<<< HEAD
// TODO
=======
>>>>>>> 23b94cf (testing fast upload)
package main

import (
	"container/heap"
	"cs425/common"
	"cs425/priqueue"
	"fmt"
	"net"
	"strings"
	"sync"
<<<<<<< HEAD
=======
	"time"
>>>>>>> 23b94cf (testing fast upload)
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

<<<<<<< HEAD
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
=======
type UploadInfo struct {
	server    net.Conn
	blockSize int
	blockData []byte
	blockName string
	replicas  []string
}

type UploadState struct {
	pendingNodes   []*Node
	busyNodes      map[string]bool
	completedNodes priqueue.PriorityQueue
	mutex          sync.Mutex
	cond           sync.Cond
}

func StartFastUpload(info *UploadInfo) bool {
	Log.Infof("Starting fast upload for block %s (%d bytes)", info.blockName, info.blockSize)

	state := new(UploadState)
	state.pendingNodes = connectAllReplica(info.replicas)
	state.busyNodes = make(map[string]bool, 0)
	state.completedNodes = make([]*priqueue.Item, 0)
	state.cond = *sync.NewCond(&state.mutex)

	heap.Init(&state.completedNodes)

	done := make(chan bool)

	go scheduler(info, state, done)
	go uploader(info, state, done)

	<-done
	<-done

	return finish(info, state)
}

func uploader(info *UploadInfo, state *UploadState, done chan bool) {
	for {
		state.mutex.Lock()

		if len(state.pendingNodes) == 0 && len(state.busyNodes) == 0 {
			state.mutex.Unlock()
			break
		}

		node := state.pendingNodes[0]
		state.pendingNodes = state.pendingNodes[1:]
		node.State = BUSY
		state.busyNodes[node.Address] = true
		state.mutex.Unlock()

		if !UploadBlock(node.Connection, info) {
			Log.Fatal("Failed to upload")
			// node.State = FAILED
			// node.Connection.Close()
		}

		state.mutex.Lock()
		Log.Info("Uploaded block to ", node.Address)
		node.State = COMPLETED
		heap.Push(&state.completedNodes, &priqueue.Item{Key: node.NumUploads, Value: node})
		state.mutex.Unlock()

		time.Sleep(1 * time.Second)
	}

	done <- true
}

func scheduler(info *UploadInfo, state *UploadState, done chan bool) {
	for {
		state.mutex.Lock()
		if len(state.pendingNodes) == 0 && len(state.busyNodes) == 0 {
			state.mutex.Unlock()
			break
		}

		if len(state.pendingNodes) > 0 && len(state.completedNodes) > 0 {
			completedNode := heap.Pop(&state.completedNodes).(*priqueue.Item).Value.(*Node)
			completedNode.State = BUSY

			pendingNode := state.pendingNodes[0]
			state.pendingNodes = state.pendingNodes[1:]
			pendingNode.State = BUSY

			completedNode.UploadingTo = pendingNode

			state.busyNodes[completedNode.Address] = true
			state.busyNodes[pendingNode.Address] = true

			state.mutex.Unlock()

			go copyBlock(info, state, pendingNode, completedNode)

			state.mutex.Lock()
		}

		state.mutex.Unlock()
		time.Sleep(1 * time.Second)
	}

	done <- true
}

func finish(info *UploadInfo, state *UploadState) bool {
	ret := []string{}

	for _, item := range state.completedNodes {
>>>>>>> 23b94cf (testing fast upload)
		node := item.Value.(*Node)
		node.Connection.Close()
		ret = append(ret, node.Address)
	}

<<<<<<< HEAD
	line := fmt.Sprintf("%s %s\n", blockName, strings.Join(ret, ","))
	if common.SendAll(server, []byte(line), len(line)) < 0 {
		return false
	}

	if len(completedNodes) < MIN_REPLICAS {
		Log.Debug("No replicas received the block")
=======
	line := fmt.Sprintf("%s %s\n", info.blockName, strings.Join(ret, ","))

	if common.SendAll(info.server, []byte(line), len(line)) < 0 {
>>>>>>> 23b94cf (testing fast upload)
		return false
	}

	return true
}

<<<<<<< HEAD
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
=======
func UploadBlock(conn net.Conn, info *UploadInfo) bool {
	_, err := conn.Write([]byte(fmt.Sprintf("UPLOAD %s %d\n", info.blockName, info.blockSize)))
	if err != nil {
		return false
	}

	Log.Debug("Sent upload block request to ", conn.RemoteAddr())

	if !common.GetOKMessage(conn) {
		return false
	}

	Log.Debug("Got OK from ", conn.RemoteAddr())

	if common.SendAll(conn, info.blockData[:info.blockSize], info.blockSize) < 0 {
		return false
	}

	Log.Debugf("Sent block (%d bytes) to %s\n", info.blockSize, conn.RemoteAddr())
	return true
}

func copyBlock(info *UploadInfo, state *UploadState, dest *Node, src *Node) {
	Log.Debugf("Starting transfer from %s to %s\n", src.Address, dest.Address)
	request := fmt.Sprintf("ADD_BLOCK %s %d %s\n", info.blockName, info.blockSize, src.Address)
	_, err := dest.Connection.Write([]byte(request))
	if err != nil {
		// handleFailure(dest)
		Log.Fatal("failed due to ", dest.Address)
		return
	}
	if !common.GetOKMessage(dest.Connection) {
		Log.Fatal("failed due to ", dest.Address)
		// handleFailure(dest)
		return
	}

	state.mutex.Lock()
>>>>>>> 23b94cf (testing fast upload)

	src.State = COMPLETED
	src.NumUploads++
	src.UploadingTo = nil

	dest.State = COMPLETED

<<<<<<< HEAD
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
=======
	heap.Push(&state.completedNodes, &priqueue.Item{Key: src.NumUploads, Value: src})
	heap.Push(&state.completedNodes, &priqueue.Item{Key: dest.NumUploads, Value: dest})

	state.mutex.Unlock()
>>>>>>> 23b94cf (testing fast upload)
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
<<<<<<< HEAD
=======

// func handleFailure(node *Node) {
// 	mutex.Lock()
// 	defer mutex.Unlock()
// 	node.State = FAILED
// 	if node.UploadingTo != nil {
// 		pendingNodes = append(pendingNodes, node.UploadingTo)
// 	}
// }

>>>>>>> 23b94cf (testing fast upload)
