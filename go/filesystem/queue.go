package filesystem

import (
	"sync"
	log "github.com/sirupsen/logrus"
)

const (
	READING = 0
	WRITING = 1
)

// The queue is enforces the following polices:
// 1. At most one writer at a time per file
// 2. At most two readers at a time per file
// 3. There can be maximum 4 reads or 4 writes consecutively per file
type Queue struct {
	Reads     []*Request
	Writes    []*Request
	Mode      int
	Count     int
	NumReader int
	NumWriter int
	Mutex     sync.Mutex
}

// Enqueue a read task for file
func (q *Queue) PushRead(request *Request) {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	q.Reads = append(q.Reads, request)
	// log.Debug("Read task added")
}

// Enqueue a write task for file
func (q *Queue) PushWrite(request *Request) {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	q.Writes = append(q.Writes, request)
	// log.Debug("Write task added")
}

// Get the next task if available, otherwise returns nil
func (q *Queue) TryPop() *Request {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()

	if q.NumReader == 2 || q.NumWriter == 1 {
		return nil
	}

	if q.Mode == READING {
		if len(q.Reads) == 0 || q.Count >= 4 {
			q.Mode = WRITING
			q.Count = 0
		} else if q.NumWriter == 0 {
			res := q.Reads[0]
			q.Reads = q.Reads[1:]
			q.NumReader++
			log.Debug("A read task was dequeued!")
			// log.Debug("Num readers: ", q.NumReader)
			return res
		}
	}

	if q.Mode == WRITING {
		if len(q.Writes) == 0 || q.Count >= 4 {
			q.Mode = READING
			q.Count = 0
		} else if q.NumReader == 0 {
			res := q.Writes[0]
			q.Writes = q.Writes[1:]
			q.NumWriter++
			log.Debug("A write task was dequeued!")
			// log.Debug("Num writers: ", q.NumWriter)
			return res
		}
	}

	return nil
}

// Must call this after reader is done
func (q *Queue) ReadDone() {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	q.Count++
	q.NumReader--
	log.Debug("A read task was completed!")
}

// Must call this after writer is done
func (q *Queue) WriteDone() {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	q.Count++
	q.NumWriter--
	log.Debug("A write task was completed!")
}
