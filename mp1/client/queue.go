package main

import (
	"sync"
)

type Queue[T any] struct {
	data    []T
	m       sync.Mutex
	cv      sync.Cond
	waiting int
}

func (q *Queue[T]) init() {
	q.cv = *sync.NewCond(&q.m)
}

func (q *Queue[T]) push(value T) {
	q.m.Lock()
	q.data = append(q.data, value)
	q.waiting = 0
	q.cv.Signal()
	q.m.Unlock()
}

func (q *Queue[T]) pop() T {
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

func (q *Queue[T]) empty() bool {
	q.m.Lock()
	if len(q.data) == 0 {
		q.m.Unlock()
		return true
	}
	q.m.Unlock()
	return false
}
