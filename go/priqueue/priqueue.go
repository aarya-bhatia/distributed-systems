package priqueue

// Item represents an item in the priority queue.
type Item struct {
	Key    int
	TieKey int
	Value  interface{}
}

// PriorityQueue implements a min-heap for Item.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].Key == pq[j].Key {
		return pq[i].TieKey < pq[j].TieKey
	}

	return pq[i].Key < pq[j].Key
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*Item)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
