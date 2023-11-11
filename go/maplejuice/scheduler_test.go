package maplejuice

import (
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

type Example struct{}

func (e Example) Start(w string, data TaskData) bool {
	log.Println("running example task")
	time.Sleep(3 * time.Second)
	return true
}

func (e Example) Restart(w string) bool {
	log.Println("running example task")
	time.Sleep(3 * time.Second)
	return true
}

func TestScheduler(t *testing.T) {
	log.Println("Begin")

	s := NewScheduler()

	s.AddWorker("a")
	s.AddWorker("b")

	e := Example{}

	s.PutTask(e, nil)
	s.PutTask(e, nil)
	s.PutTask(e, nil)
	s.PutTask(e, nil)

	s.Wait()

	log.Println("End")
}
