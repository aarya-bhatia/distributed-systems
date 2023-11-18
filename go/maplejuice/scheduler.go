package maplejuice

import (
	"cs425/common"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const SCHEDULER_POLL_INTERVAL = 100 * time.Millisecond

type Task interface {
	Start(worker int) bool
	Hash() int
	GetID() int64
}

type Scheduler struct {
	Tasks    map[int64]Task
	Workers  map[int][]int64
	Mutex    sync.Mutex
	NumTasks int
}

func NewScheduler() *Scheduler {
	s := new(Scheduler)
	s.Workers = make(map[int][]int64)
	s.Tasks = make(map[int64]Task)
	s.NumTasks = 0
	return s
}

func (s *Scheduler) AddWorker(worker int) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	log.Debug("Added worker", worker)
	if _, ok := s.Workers[worker]; !ok {
		s.Workers[worker] = make([]int64, 0)
	}
}

func (s *Scheduler) RemoveWorker(worker int) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	for _, task := range s.Workers[worker] {
		s.AssignTask(s.Tasks[task])
	}
	log.Debug("Removed worker", worker)
}

func (s *Scheduler) AssignTask(task Task) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	taskID := task.GetID()
	s.Tasks[taskID] = task
	workers := make([]int, 0, len(s.Workers))
	for k := range s.Workers {
		workers = append(workers, k)
	}
	worker := workers[task.Hash()%len(workers)]
	s.Workers[worker] = append(s.Workers[worker], taskID)
	s.NumTasks++
	log.Println("Task assigned to worker", worker)
	go task.Start(worker)
}

func (s *Scheduler) TaskDone(worker int, taskID int64, status bool) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	log.Printf("Ack (%v) from worker %d for task %d", status, worker, taskID)
	s.Workers[worker] = common.RemoveElement(s.Workers[worker], taskID)
	if status == true {
		delete(s.Tasks, taskID)
		s.NumTasks--
	} else {
		s.AssignTask(s.Tasks[taskID])
	}
}

func (s *Scheduler) Wait() {
	for {
		s.Mutex.Lock()
		if s.NumTasks == 0 {
			s.Mutex.Unlock()
			return
		}
		s.Mutex.Unlock()
		time.Sleep(SCHEDULER_POLL_INTERVAL)
	}
}
