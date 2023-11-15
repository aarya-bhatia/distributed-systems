package client

import (
	"errors"
	"io"
	"os"
)

type Reader interface {
	Read(blockSize int) ([]byte, error) // read next chunk of data
	Size() int                          // total bytes
	Open() error
	Close()
}

type ByteReader struct {
	data   []byte
	offset int
}

type FileReader struct {
	filename string
	file     *os.File
	info     os.FileInfo
}

func NewByteReader(data []byte) *ByteReader {
	return &ByteReader{data: data, offset: 0}
}

func (r *ByteReader) Size() int {
	return len(r.data)
}

func (r *ByteReader) Open() error {
	r.offset = 0
	return nil
}

func (r *ByteReader) Close() {
}

func (r *ByteReader) Read(blockSize int) ([]byte, error) {
	ret := make([]byte, blockSize)
	n := copy(ret, r.data[r.offset:])
	r.offset += n
	return ret[:n], nil
}

func NewFileReader(filename string) (*FileReader, error) {
	r := new(FileReader)
	r.filename = filename
	return r, nil
}

func (r *FileReader) Size() int {
	return int(r.info.Size())
}

func (r *FileReader) Read(blockSize int) ([]byte, error) {
	buffer := make([]byte, blockSize)
	n, err := r.file.Read(buffer)
	if err != nil {
		return nil, err
	}
	return buffer[:n], nil
}

func (r *FileReader) Open() error {
	if r.file != nil {
		r.file.Seek(0, io.SeekStart)
		return nil
	}
	info, err := os.Stat(r.filename)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return errors.New("Not a regular file")
	}
	file, err := os.Open(r.filename)
	if err != nil {
		return err
	}
	r.info = info
	r.file = file
	return nil
}

func (r *FileReader) Close() {
	r.file.Close()
	r.file = nil
}
