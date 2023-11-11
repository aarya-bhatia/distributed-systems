package maplejuice

import (
	"cs425/common"
	log "github.com/sirupsen/logrus"
	"sync"
)

type Scheduler struct {
	BusyWorkers map[string]Task
	IdleWorkers []string
	Mutex       sync.Mutex
	CV          sync.Cond
	Tasks       []Task
}

func NewScheduler() *Scheduler {
	s := new(Scheduler)
	s.BusyWorkers = make(map[string]Task, 0)
	s.IdleWorkers = make([]string, 0)
	s.Tasks = make([]Task, 0)
	s.CV = *sync.NewCond(&s.Mutex)
	return s
}

func (s *Scheduler) StartTask(worker string, task Task, data TaskData) {
	log.Println("task started")
	if task.Start(worker, data) {
		s.TaskDone(worker)
	} else {
		s.TaskFail(worker)
	}
	log.Println("task finished")
}

func (s *Scheduler) RestartTask(worker string, task Task) {
	if task.Restart(worker) {
		s.TaskDone(worker)
	} else {
		s.TaskFail(worker)
	}
}

func (s *Scheduler) AddWorker(worker string) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	defer s.CV.Broadcast()
	s.IdleWorkers = append(s.IdleWorkers, worker)
	log.Debug("Added worker", worker)
}

func (s *Scheduler) RemoveWorker(worker string) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	defer s.CV.Broadcast()
	if task, ok := s.BusyWorkers[worker]; ok {
		s.Tasks = append(s.Tasks, task)
		delete(s.BusyWorkers, worker)
	}
	s.IdleWorkers = common.RemoveElement(s.IdleWorkers, worker)
	log.Debug("Removed worker", worker)
}

func (s *Scheduler) TaskDone(worker string) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	defer s.CV.Broadcast()
	delete(s.BusyWorkers, worker)
	s.IdleWorkers = common.AddUniqueElement(s.IdleWorkers, worker)
	log.Debug("Task done by worker", worker)
}

func (s *Scheduler) TaskFail(worker string) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	defer s.CV.Broadcast()
	if t, ok := s.BusyWorkers[worker]; ok {
		s.Tasks = append(s.Tasks, t)
		delete(s.BusyWorkers, worker)
		log.Debug("Task failed by worker", worker)
	}
}

func (s *Scheduler) PutTask(task Task, data TaskData) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	log.Debug("waiting for workers..")
	for len(s.IdleWorkers) == 0 {
		s.CV.Wait()
	}
	worker := s.IdleWorkers[0]
	s.IdleWorkers = s.IdleWorkers[1:]
	s.BusyWorkers[worker] = task

	go s.StartTask(worker, task, data)
}

func (s *Scheduler) Wait() {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	for {
		if len(s.BusyWorkers) == 0 && len(s.Tasks) == 0 {
			log.Debug("all tasks finished.")
			return
		}

		log.Debug("waiting for tasks to finish...")
		s.CV.Wait()

		for len(s.Tasks) > 0 && len(s.IdleWorkers) > 0 {
			log.Debug("rescheduling task")
			task := s.Tasks[0]
			s.Tasks = s.Tasks[1:]
			worker := s.IdleWorkers[0]
			s.IdleWorkers = s.IdleWorkers[1:]
			s.BusyWorkers[worker] = task
			go s.RestartTask(worker, task)
		}
	}
}
