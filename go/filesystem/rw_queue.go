package filesystem

import (
	"sync"
	// "time"
)

type RWQueue struct {
	Reads       []*Request
	Writes      []*Request
	WriterCount int
	ReaderCount int
	TotalReads  int
	TotalWrites int

	Mutex sync.Mutex
	Cond  sync.Cond
}

func NewRWQueue() *RWQueue {
	rw := new(RWQueue)
	rw.WriterCount = 0
	rw.ReaderCount = 0
	rw.TotalReads = 0
	rw.TotalWrites = 0
	rw.Cond = *sync.NewCond(&rw.Mutex)

	return rw
}

func (rw *RWQueue) Info() {
	Log.Debugf("RWQueue: Queued: %d to read, %d to write", len(rw.Reads), len(rw.Writes))
	Log.Debugf("RWQueue: Currently: %d reading, %d writing", rw.ReaderCount, rw.WriterCount)
	Log.Debugf("RWQueue: Consecutive: %d reads, %d writes", rw.TotalReads, rw.TotalWrites)
}

func (rw *RWQueue) PushRead(request *Request) {
	rw.Mutex.Lock()
	defer rw.Mutex.Unlock()
	rw.Reads = append(rw.Reads, request)
	rw.Info()
	rw.Cond.Broadcast()
}

func (rw *RWQueue) PushWrite(request *Request) {
	rw.Mutex.Lock()
	defer rw.Mutex.Unlock()
	rw.Writes = append(rw.Writes, request)
	rw.Info()
	rw.Cond.Broadcast()
}

func (rw *RWQueue) Pop() *Request {
	rw.Mutex.Lock()
	defer rw.Mutex.Unlock()

	rw.Info()

	for len(rw.Reads) == 0 && len(rw.Writes) == 0 {
		Log.Debug("Waiting for new requests")
		rw.Cond.Wait()
	}

	for rw.ReaderCount == 2 || rw.WriterCount == 1 {
		Log.Debug("Waiting for readers or writers to finish!")
		rw.Cond.Wait()
	}

	read := false

	if len(rw.Reads) > 0 {
		if len(rw.Writes) == 0 {
			read = true
		} else {
			if rw.Reads[0].Timestamp < rw.Writes[0].Timestamp {
				read = true
			}

			// if rw.TotalReads >= 4 && rw.TotalWrites >= 4 {
			// 	if rw.Reads[0].Timestamp < rw.Writes[0].Timestamp {
			// 		read = true
			// 	}
			// } else if rw.TotalWrites >= 4 {
			// 	read = true
			// } else {
			// 	read = true
			// }
		}
	}

	var res *Request

	if read {
		res = rw.Reads[0]
		rw.ReaderCount++
		rw.Reads = rw.Reads[1:]

		if rw.TotalWrites >= 4 {
			rw.TotalWrites = 0
			Log.Debug("Reset consecutive write counter")
		}

		Log.Debug("A read task was dequeued!")
	} else {
		res = rw.Writes[0]
		rw.WriterCount++
		rw.Writes = rw.Writes[1:]

		if rw.TotalReads >= 4 {
			rw.TotalReads = 0
			Log.Debug("Reset consecutive read counter")
		}

		Log.Debug("A write task was dequeued!")
	}

	rw.Info()
	return res
}

func (rw *RWQueue) ReadDone() {
	rw.Mutex.Lock()
	defer rw.Mutex.Unlock()
	rw.TotalReads++
	rw.ReaderCount--
	Log.Debug("Read done!")
	rw.Info()
	rw.Cond.Broadcast()
}

func (rw *RWQueue) WriteDone() {
	rw.Mutex.Lock()
	defer rw.Mutex.Unlock()
	rw.TotalWrites++
	rw.WriterCount--
	Log.Debug("Write done!")
	rw.Info()
	rw.Cond.Broadcast()
}
