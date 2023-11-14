package server

import (
	"cs425/common"
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	READ  = 0
	WRITE = 1
)

type Resource struct {
	Name      string
	Mode      int
	Heartbeat int64
}

type ResourceManager struct {
	FileQueues map[string]*Queue
	Clients    map[string]Resource
	Mutex      sync.Mutex
}

func NewResourceManager() *ResourceManager {
	rm := new(ResourceManager)
	rm.FileQueues = make(map[string]*Queue)
	rm.Clients = make(map[string]Resource)
	return rm
}

// To periodically dequeue available tasks
func (rm *ResourceManager) StartTaskPolling() {
	for {
		rm.Mutex.Lock()
		for _, queue := range rm.FileQueues {
			// Pop all tasks from queue
			for queue.TryPop() {
			}
		}
		rm.Mutex.Unlock()
		time.Sleep(common.POLL_INTERVAL)
	}
}

// To detect failed client and release their resource
func (rm *ResourceManager) StartHeartbeatRoutine() {
	for {
		rm.Mutex.Lock()
		timeNow := time.Now().UnixNano()
		for client, resource := range rm.Clients {
			if timeNow-resource.Heartbeat > common.CLIENT_TIMEOUT.Nanoseconds() {
				log.Println("client failure detected:", client, resource)
				if resource.Mode == READ {
					rm.FileQueues[resource.Name].ReadDone()
				} else {
					rm.FileQueues[resource.Name].WriteDone()
				}
				delete(rm.Clients, client)
			}
		}
		rm.Mutex.Unlock()
		time.Sleep(common.POLL_INTERVAL)
	}
}

// Get or create a queue to handle requests for given file
func (rm *ResourceManager) getQueue(filename string) *Queue {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	q, ok := rm.FileQueues[filename]
	if !ok {
		q = new(Queue)
		rm.FileQueues[filename] = q
	}

	return q
}

func (rm *ResourceManager) getWriteLock(filename string) {
	c := make(chan bool)
	rm.getQueue(filename).PushWrite(c)
	<-c
}

func (rm *ResourceManager) getReadLock(filename string) {
	c := make(chan bool)
	rm.getQueue(filename).PushRead(c)
	<-c
}

func (rm *ResourceManager) Ping(clientID string, resource string) error {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	if _, ok := rm.Clients[clientID]; !ok {
		log.Warn("unauthorized")
		return errors.New("unauthorized")
	}

	if rm.Clients[clientID].Name != resource {
		log.Warn("client resource mismatch")
		return errors.New("client resource mismatch")
	}

	r := rm.Clients[clientID]
	r.Heartbeat = time.Now().UnixNano()
	rm.Clients[clientID] = r

	return nil
}

func (rm *ResourceManager) Acquire(clientID string, resource string, mode int) error {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	if _, ok := rm.Clients[clientID]; ok {
		log.Warn("clientID is unavailable")
		return errors.New("clientID is unavailable")
	}

	rm.Mutex.Unlock()
	if mode == READ {
		log.Debug("Waiting for read lock...")
		rm.getReadLock(resource)
	} else {
		log.Debug("Waiting for write lock...")
		rm.getWriteLock(resource)
	}
	rm.Mutex.Lock()

	rm.Clients[clientID] = Resource{
		Name:      resource,
		Mode:      mode,
		Heartbeat: time.Now().UnixNano(),
	}

	log.Println("Resource was acquired:", resource)
	return nil
}

func (rm *ResourceManager) Release(clientID string, resource string) error {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	if _, ok := rm.Clients[clientID]; !ok {
		log.Warn("unauthorized")
		return errors.New("unauthorized")
	}

	if rm.Clients[clientID].Name != resource {
		log.Warn("client resource mismatch")
		return errors.New("client resource mismatch")
	}

	res := rm.Clients[clientID]

	rm.Mutex.Unlock()
	if res.Mode == READ {
		rm.FileQueues[res.Name].ReadDone()
	} else {
		rm.FileQueues[res.Name].WriteDone()
	}
	rm.Mutex.Lock()

	delete(rm.Clients, clientID)
	log.Println("Resource was released:", resource)
	return nil
}
