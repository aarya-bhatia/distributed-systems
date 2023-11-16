package maplejuice

import (
	"bufio"
	"strings"
)

type Splitter struct {
	Reader   *bufio.Reader
	Finished bool
}

func NewSplitter(data string) *Splitter {
	return &Splitter{
		Reader:   bufio.NewReader(strings.NewReader(data)),
		Finished: false,
	}
}

func (fs *Splitter) Next(count int) ([]string, bool) {
	if fs.Finished {
		return nil, false
	}

	lines := []string{}

	for i := 0; i < count; i++ {
		line, err := fs.Reader.ReadString('\n')
		if err != nil {
			fs.Finished = true
			break
		}

		lines = append(lines, line[:len(line)-1])
	}

	return lines, true
}
