package priqueue

import (
	"container/heap"
	"fmt"
	"testing"
)

func TestPriorityQueue(t *testing.T) {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)

	// Push items into the priority queue
	items := []*Item{
		{Key: 3, Value: 42},
		{Key: 1, Value: 20},
		{Key: 2, Value: 10},
		{Key: 5, Value: 5},
	}

	for _, item := range items {
		heap.Push(&pq, item)
	}

	// Pop items from the priority queue in ascending order of key
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*Item)
		fmt.Printf("Key: %d, Value: %d\n", item.Key, item.Value)
	}
}
