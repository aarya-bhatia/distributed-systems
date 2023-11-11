package maplejuice

import (
	"testing"
	log "github.com/sirupsen/logrus"
)

func TestScheduler(t *testing.T) {
	log.Println("Begin")

	s := NewScheduler()

	s.AddWorker("a")
	s.AddWorker("b")

	s.PutTask(Task{})
	s.PutTask(Task{})
	s.PutTask(Task{})
	s.PutTask(Task{})

	s.Wait()

	log.Println("End")
}
