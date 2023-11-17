package maplejuice

import (
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

type Example struct{}

func (e Example) Start(w int) bool {
	log.Printf("worker %d running example task", w)
	time.Sleep(3 * time.Second)
	return true
}

func TestScheduler(t *testing.T) {
	log.Println("Begin")

	s := NewScheduler()

	s.AddWorker(12)
	s.AddWorker(13)

	e := Example{}

	s.PutTask(e)
	s.PutTask(e)
	s.PutTask(e)
	s.PutTask(e)

	s.Wait()

	log.Println("End")
}
