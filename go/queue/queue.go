// A thread safe and generic queue data structure
package queue

import (
	"sync"
)

type Queue struct {
	data    []interface{}
	m       sync.Mutex
	cv      sync.Cond
	waiting int
}

func NewQueue() *Queue {
	q := new(Queue)
	q.cv = *sync.NewCond(&q.m)
	return q
}

func (q *Queue) Push(value interface{}) {
	q.m.Lock()
	q.data = append(q.data, value)
	q.waiting = 0
	q.cv.Signal()
	q.m.Unlock()
}

func (q *Queue) Pop() interface{} {
	q.m.Lock()
	q.waiting += 1
	for len(q.data) == 0 {
		q.cv.Wait()
	}
	value := q.data[0]
	q.data = q.data[1:]
	q.waiting -= 1
	if q.waiting > 0 {
		q.cv.Signal()
	}
	q.m.Unlock()
	return value
}

func (q *Queue) IsEmpty() bool {
	q.m.Lock()
	if len(q.data) == 0 {
		q.m.Unlock()
		return true
	}
	q.m.Unlock()
	return false
}
