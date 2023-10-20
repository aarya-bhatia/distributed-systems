package main

import (
	"container/heap"
	"cs425/common"
	"cs425/priqueue"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
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

const UPLOADER_INTERVAL = 400 * time.Millisecond
const SCHEDULER_INTERVAL = 100 * time.Millisecond

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
	c := 0
	for {
		state.mutex.Lock()

		if len(state.pendingNodes) == 0 && len(state.busyNodes) == 0 {
			state.mutex.Unlock()
			break
		}

		if len(state.pendingNodes) > 0 {
			node := state.pendingNodes[0]
			state.pendingNodes = state.pendingNodes[1:]
			node.State = BUSY
			state.busyNodes[node.Address] = true
			Log.Debug("uploading block to ", node.Address)
			state.mutex.Unlock()

			if !UploadBlock(node.Connection, info) {
				Log.Fatal("Failed to upload")
				// node.State = FAILED
				// node.Connection.Close()
			}

			state.mutex.Lock()
			c++
			Log.Info("Uploaded block to ", node.Address)
			delete(state.busyNodes, node.Address)
			node.State = COMPLETED
			heap.Push(&state.completedNodes, &priqueue.Item{Key: node.NumUploads, Value: node})
		}

		Log.Debug("uploader is waiting...")
		state.mutex.Unlock()

		time.Sleep(UPLOADER_INTERVAL)
	}

	Log.Info("Uploader is done with counter: ", c)
	done <- true
}

func scheduler(info *UploadInfo, state *UploadState, done chan bool) {
	c := 0
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

			Log.Debug("scheduler is starting a job...")
			go copyBlock(info, state, pendingNode, completedNode)
			c++

			state.mutex.Lock()
		}

		state.mutex.Unlock()

		Log.Debug("scheduler is waiting...")
		time.Sleep(1 * time.Second)
	}

	Log.Info("Scheduler is done with counter: ", c)
	done <- true
}

func finish(info *UploadInfo, state *UploadState) bool {
	ret := []string{}

	for _, item := range state.completedNodes {
		node := item.Value.(*Node)
		node.Connection.Close()
		ret = append(ret, node.Address)
	}

	line := fmt.Sprintf("%s %s\n", info.blockName, strings.Join(ret, ","))

	if common.SendAll(info.server, []byte(line), len(line)) < 0 {
		return false
	}

	return true
}

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

	src.State = COMPLETED
	src.NumUploads++
	src.UploadingTo = nil

	dest.State = COMPLETED

	delete(state.busyNodes, src.Address)
	delete(state.busyNodes, dest.Address)

	heap.Push(&state.completedNodes, &priqueue.Item{Key: src.NumUploads, Value: src})
	heap.Push(&state.completedNodes, &priqueue.Item{Key: dest.NumUploads, Value: dest})

	Log.Debugf("Block transfer from %s to %s has finished!\n", src.Address, dest.Address)
	state.mutex.Unlock()
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

// func handleFailure(node *Node) {
// 	mutex.Lock()
// 	defer mutex.Unlock()
// 	node.State = FAILED
// 	if node.UploadingTo != nil {
// 		pendingNodes = append(pendingNodes, node.UploadingTo)
// 	}
// }
