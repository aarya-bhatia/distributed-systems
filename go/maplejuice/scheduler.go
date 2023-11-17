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
}

type Scheduler struct {
	BusyWorkers map[int]Task
	IdleWorkers []int
	Mutex       sync.Mutex
	Tasks       []Task
}

func NewScheduler() *Scheduler {
	s := new(Scheduler)
	s.BusyWorkers = make(map[int]Task, 0)
	s.IdleWorkers = make([]int, 0)
	s.Tasks = make([]Task, 0)
	return s
}

func (s *Scheduler) StartTask(worker int, task Task) {
	log.Println("task started")
	defer log.Println("task finished")

	if task.Start(worker) {
		s.TaskDone(worker)
	} else {
		s.TaskFail(worker)
	}
}

func (s *Scheduler) AddWorker(worker int) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.IdleWorkers = append(s.IdleWorkers, worker)
	log.Debug("Added worker", worker)
}

func (s *Scheduler) RemoveWorker(worker int) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if task, ok := s.BusyWorkers[worker]; ok {
		s.Tasks = append(s.Tasks, task)
		delete(s.BusyWorkers, worker)
	}
	s.IdleWorkers = common.RemoveElement(s.IdleWorkers, worker)
	log.Debug("Removed worker", worker)
}

func (s *Scheduler) TaskDone(worker int) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	delete(s.BusyWorkers, worker)
	if !common.HasElement(s.IdleWorkers, worker) {
		s.IdleWorkers = append(s.IdleWorkers, worker)
	}
	log.Debug("Task done by worker", worker)
}

func (s *Scheduler) TaskFail(worker int) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if t, ok := s.BusyWorkers[worker]; ok {
		s.Tasks = append(s.Tasks, t)
		delete(s.BusyWorkers, worker)
		log.Debug("Task failed by worker", worker)
	}
}

func (s *Scheduler) PutTask(task Task) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.Tasks = append(s.Tasks, task)
	log.Debug("Task added:", task)
}

func (s *Scheduler) Wait() {
	for {
		s.Mutex.Lock()
		if len(s.BusyWorkers) == 0 && len(s.Tasks) == 0 {
			log.Debug("all tasks finished.")
			s.Mutex.Unlock()
			return
		}

		log.Debug("waiting for tasks to finish...")

		for len(s.Tasks) > 0 && len(s.IdleWorkers) > 0 {
			task := s.Tasks[0]
			worker := s.IdleWorkers[0]

			s.Tasks = s.Tasks[1:]
			s.IdleWorkers = s.IdleWorkers[1:]

			s.BusyWorkers[worker] = task

			log.Debug("Sending task", task, "to worker", worker)
			go s.StartTask(worker, task)
		}

		s.Mutex.Unlock()
		time.Sleep(SCHEDULER_POLL_INTERVAL)
	}
}
