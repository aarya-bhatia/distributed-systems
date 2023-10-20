package filesystem

import (
	"sync"
)

const (
	READING = 0
	WRITING = 1
)

type Queue struct {
	Reads     []*Request
	Writes    []*Request
	Mode      int
	Count     int
	NumReader int
	NumWriter int
	Mutex     sync.Mutex
}

func (q *Queue) PushRead(request *Request) {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	q.Reads = append(q.Reads, request)
	Log.Debug("Read task added")
}

func (q *Queue) PushWrite(request *Request) {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	q.Writes = append(q.Writes, request)
	Log.Debug("Write task added")
}

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
		} else {
			res := q.Reads[0]
			q.Reads = q.Reads[1:]
			q.NumReader++
			Log.Debug("A read task was dequeued!")
			Log.Debug("Num readers: ", q.NumReader)
			return res
		}
	}

	if q.Mode == WRITING {
		if len(q.Writes) == 0 || q.Count >= 4 {
			q.Mode = READING
			q.Count = 0
		} else {
			res := q.Writes[0]
			q.Writes = q.Writes[1:]
			q.NumWriter++
			Log.Debug("A write task was dequeued!")
			Log.Debug("Num writers: ", q.NumWriter)
			return res
		}
	}

	return nil
}

func (q *Queue) ReadDone() {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	q.Count++
	q.NumReader--
	Log.Debug("A read task was completed!")
}

func (q *Queue) WriteDone() {
	q.Mutex.Lock()
	defer q.Mutex.Unlock()
	q.Count++
	q.NumWriter--
	Log.Debug("A write task was completed!")
}
