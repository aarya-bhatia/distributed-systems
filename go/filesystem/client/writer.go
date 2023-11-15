package client

import (
	"io"
	"os"
)

type Writer interface {
	Write([]byte) (int, error) // consume next chunk of data
	Close()
	Open() error
}

type FileWriter struct {
	filename string
	file     *os.File
}

type ByteWriter struct {
	Data   []byte
	Offset int
}

func NewFileWriter(filename string) (*FileWriter, error) {
	w := new(FileWriter)
	w.filename = filename
	return w, nil
}

func (w *FileWriter) Write(data []byte) (int, error) {
	return w.file.Write(data)
}

func (w *FileWriter) Open() error {
	if w.file != nil {
		w.file.Seek(0, io.SeekStart)
		return nil
	}

	file, err := os.OpenFile(w.filename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
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
	w.Offset = 0
	return nil
}

func (w *ByteWriter) Write(data []byte) (int, error) {
	n := copy(data, w.Data[w.Offset:])
	w.Offset += n
	return n, nil
}

func (w *ByteWriter) String() string {
	return string(w.Data)
}

func (w *ByteWriter) Close() {
}
