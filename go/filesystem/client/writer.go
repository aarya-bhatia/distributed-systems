package client

import (
	"io"
	"os"
)

type Writer interface {
	Write([]byte) error // consume next chunk of data
	Close()
	Open() error
}

type FileWriter struct {
	filename string
	file     *os.File
}

type ByteWriter struct {
	Data []byte
}

func NewFileWriter(filename string) (*FileWriter, error) {
	w := new(FileWriter)
	w.filename = filename
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
