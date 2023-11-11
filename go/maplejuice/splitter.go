package maplejuice

import (
	"bufio"
	"os"
	"sync"
)

type FileSplitter struct {
	Filename string
	File     *os.File
	Reader   *bufio.Reader
	Finished bool
}

type MultiFileSplitter struct {
	Filename  []string
	NumLines  int
	Splitters []*FileSplitter
	Mutex     sync.Mutex
}

func NewFileSplitter(filename string) *FileSplitter {
	file, err := os.Open(filename)
	if err != nil {
		return nil
	}

	fs := new(FileSplitter)
	fs.Filename = filename
	fs.File = file
	fs.Reader = bufio.NewReader(file)
	fs.Finished = false

	return fs
}

func (fs *FileSplitter) Next(count int) ([]string, bool) {
	if fs.Finished {
		return nil, false
	}

	lines := []string{}

	for i := 0; i < count; i++ {
		line, err := fs.Reader.ReadString('\n')
		if err != nil {
			fs.Finished = true
			fs.File.Close()
			break
		}

		lines = append(lines, line[:len(line)-1])
	}

	return lines, true
}

func NewMultiFileSplitter(numlines int) *MultiFileSplitter {
	ms := new(MultiFileSplitter)
	ms.NumLines = numlines
	ms.Splitters = make([]*FileSplitter, 0)

	return ms
}

func (ms *MultiFileSplitter) AddFile(filename string) bool {
	ms.Mutex.Lock()
	defer ms.Mutex.Unlock()

	fs := NewFileSplitter(filename)
	if fs == nil {
		return false
	}

	ms.Splitters = append(ms.Splitters, fs)
	return true
}

func (ms *MultiFileSplitter) Next() ([]string, bool) {
	ms.Mutex.Lock()
	defer ms.Mutex.Unlock()

	for len(ms.Splitters) > 0 {
		if lines, ok := ms.Splitters[0].Next(ms.NumLines); ok {
			return lines, true
		}

		ms.Splitters = ms.Splitters[1:]
	}

	return nil, false
}
