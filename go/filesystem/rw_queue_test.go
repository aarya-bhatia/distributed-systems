package filesystem

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRWQueue(t *testing.T) {
	q := NewRWQueue()
	q.PushRead(&Request{Action: 1, Name: "a"})
	q.PushRead(&Request{Action: 1, Name: "b"})

	r := q.Pop()
	assert.True(t, r.Action == 1)
	assert.True(t, r.Name == "a")

	r = q.Pop()
	assert.True(t, r.Action == 1)
	assert.True(t, r.Name == "b")

	q.PushRead(&Request{Action: 1})
	q.PushWrite(&Request{Action: 2})
	q.PushRead(&Request{Action: 1})
	q.PushWrite(&Request{Action: 2})
	q.PushRead(&Request{Action: 1})
	q.PushWrite(&Request{Action: 2})

	r = q.Pop()
	r = q.Pop()
}
