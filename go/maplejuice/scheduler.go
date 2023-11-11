package maplejuice

import (
	"cs425/common"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Task struct {
	Filename    string
	OffsetLines int
	CountLines  int
}

type Scheduler struct {
	BusyWorkers map[string]Task
	IdleWorkers []string
	Mutex       sync.Mutex
	Tasks       []Task
}

func NewScheduler() *Scheduler {
	s := new(Scheduler)
	s.BusyWorkers = make(map[string]Task, 0)
	s.IdleWorkers = make([]string, 0)
	s.Tasks = make([]Task, 0)
	return s
}

func StartTask(s *Scheduler, worker string, task Task) {
	defer s.TaskDone(worker)
	log.Println("Task started for worker", worker)
	time.Sleep(3 * time.Second)
}

func (s *Scheduler) AddWorker(worker string) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	delete(s.BusyWorkers, worker)
	s.IdleWorkers = append(s.IdleWorkers, worker)
	log.Println("Added worker", worker)
}

func (s *Scheduler) RemoveWorker(worker string) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if task, ok := s.BusyWorkers[worker]; ok {
		s.Tasks = append(s.Tasks, task)
		delete(s.BusyWorkers, worker)
	}
	s.IdleWorkers = common.RemoveElement(s.IdleWorkers, worker)
	log.Println("Removed worker", worker)
}

func (s *Scheduler) TaskDone(worker string) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	delete(s.BusyWorkers, worker)
	s.IdleWorkers = append(s.IdleWorkers, worker)
	log.Println("Task done by worker", worker)
}

func (s *Scheduler) TaskFail(worker string) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	delete(s.BusyWorkers, worker)
	log.Println("Task failed by worker", worker)
}

func (s *Scheduler) PutTask(task Task) {
	for {
		s.Mutex.Lock()
		if len(s.IdleWorkers) > 0 {
			worker := s.IdleWorkers[0]
			s.IdleWorkers = s.IdleWorkers[1:]
			s.BusyWorkers[worker] = task
			s.Mutex.Unlock()
			go StartTask(s, worker, task)
			return
		}
		s.Mutex.Unlock()
		log.Println("No idle workers.. waiting")
		time.Sleep(time.Second)
	}
}

func (s *Scheduler) Wait() {
	for {
		s.Mutex.Lock()
		if len(s.BusyWorkers) == 0 && len(s.Tasks) == 0 {
			s.Mutex.Unlock()
			return
		}
		if len(s.Tasks) > 0 && len(s.IdleWorkers) > 0 {
			log.Println("rescheduling task")
			task := s.Tasks[0]
			s.Tasks = s.Tasks[1:]
			worker := s.IdleWorkers[0]
			s.IdleWorkers = s.IdleWorkers[1:]
			s.BusyWorkers[worker] = task
			go StartTask(s, worker, task)
		}
		s.Mutex.Unlock()
		log.Println("waiting for tasks to finish...")
		time.Sleep(time.Second)
	}
}
