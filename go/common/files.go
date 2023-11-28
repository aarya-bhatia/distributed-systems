package common

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
)

type FileEntry struct {
	Name string
	Size int64
}

// Returns true if given file exists
func FileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

// Returns block name using the convention filename:blockNumber
func GetBlockName(filename string, blockNum int) string {
	return fmt.Sprintf("%s:%d", filename, blockNum)
}

// Returns the number blocks for a file of given size
func GetNumFileBlocks(fileSize int64) int {
	n := int(fileSize / BLOCK_SIZE)
	if fileSize%BLOCK_SIZE > 0 {
		n += 1
	}
	return n
}

// Writes all bytes of given file and returns true if successful
func WriteFile(filename string, flags int, buffer []byte, blockSize int) error {
	// log.Println("Flags:",flags)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|flags, 0666)
	if err != nil {
		log.Warn(err)
		return err
	}
	defer file.Close()

	_, err = file.Write(buffer[:blockSize])
	if err != nil {
		log.Warn(err)
		return err
	}

	return nil
}

// Returns all bytes of given file or nil
func ReadFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Warn(err)
		return nil, err
	}

	buffer, err := io.ReadAll(file)
	if err != nil {
		log.Warn(err)
		return nil, err
	}

	return buffer, nil
}

// Return list of file entries in directory which include name and size of file
func GetFilesInDirectory(directoryPath string) ([]FileEntry, error) {
	var files []FileEntry

	dir, err := os.Open(directoryPath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	dirEntries, err := dir.Readdir(0)
	if err != nil {
		return nil, err
	}

	for _, entry := range dirEntries {
		if !entry.IsDir() {
			files = append(files, FileEntry{entry.Name(), entry.Size()})
		}
	}

	return files, nil
}

// Return number of immediate files in directory
func GetFileCountInDirectory(directory string) int {
	c := 0

	dir, err := os.Open(directory)
	if err != nil {
		return 0
	}

	defer dir.Close()

	dirEntries, err := dir.Readdir(0)
	if err != nil {
		return 0
	}

	for _, entry := range dirEntries {
		if !entry.IsDir() {
			c++
		}
	}

	return c
}
