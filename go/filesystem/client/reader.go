package client

import "os"

type Reader interface {
	Read(blockSize int) ([]byte, error) // read next chunk of data
	Size() int                          // total bytes
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

func (r *ByteReader) Read(blockSize int) ([]byte, error) {
	if len(r.data) < blockSize {
		ret := r.data[r.offset:]
		r.offset += len(r.data)
		return ret, nil
	}

	ret := r.data[r.offset : r.offset+blockSize]
	r.offset += blockSize
	return ret, nil
}

func NewFileReader(filename string) (*FileReader, error) {
	r := new(FileReader)
	r.filename = filename
	info, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	r.info = info
	r.file = file
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
