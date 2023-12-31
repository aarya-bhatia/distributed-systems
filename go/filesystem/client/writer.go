package client

import (
	"io"
	"io/fs"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

const DEFAULT_MODE_BITS = 0666
const DEFAULT_FILE_FLAGS = os.O_WRONLY | os.O_TRUNC | os.O_CREATE

type Writer interface {
	Write([]byte) error // consume next chunk of data
	Close()
	Open() error
}

type TestWriter struct {
	StartTimeNano int64
	EndTimeNano   int64
	NumBytes      int
	NumBlocks     int
}

type FileWriter struct {
	filename string
	file     *os.File
	mode     fs.FileMode
	flags    int
}

type ByteWriter struct {
	Data []byte
}

func (w *TestWriter) Write(data []byte) error {
	log.Println("TestWriter: Write()")
	w.NumBytes += len(data)
	w.NumBlocks++
	return nil
}
func (w *TestWriter) Close() {
	log.Println("TestWriter: Close()")
	w.EndTimeNano = time.Now().UnixNano()
}

func (w *TestWriter) Open() error {
	log.Println("TestWriter: Open()")
	w.NumBytes = 0
	w.NumBlocks = 0
	w.StartTimeNano = time.Now().UnixNano()
	return nil
}

func NewFileWriter(filename string) (*FileWriter, error) {
	w := new(FileWriter)
	w.filename = filename
	w.flags = DEFAULT_FILE_FLAGS
	w.mode = DEFAULT_MODE_BITS
	return w, nil
}

func NewFileWriterWithOpts(filename string, flags int, filemode fs.FileMode) (*FileWriter, error) {
	w := new(FileWriter)
	w.filename = filename
	w.flags = flags
	w.mode = filemode
	return w, nil
}

func (w *FileWriter) Write(data []byte) error {
	_, err := w.file.Write(data)
	return err
}

func (w *FileWriter) Open() error {
	if w.file != nil {
		w.file.Seek(0, io.SeekStart)
		return nil
	}

	file, err := os.OpenFile(w.filename, w.flags, w.mode)
	if err != nil {
		return err
	}
	w.file = file
	return nil
}

func (w *FileWriter) Close() {
	w.file.Close()
	w.file = nil
}

func NewByteWriter() *ByteWriter {
	return new(ByteWriter)
}

func (w *ByteWriter) Open() error {
	w.Data = []byte{}
	return nil
}

func (w *ByteWriter) Write(data []byte) error {
	newData := make([]byte, len(w.Data)+len(data))
	n := copy(newData, w.Data)
	copy(newData[n:], data)
	w.Data = newData
	return nil
}

func (w *ByteWriter) String() string {
	return string(w.Data)
}

func (w *ByteWriter) Close() {
}
