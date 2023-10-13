package main

import (
	"fmt"
	"os"
)

// Returns the number blocks for a file of given size
func GetNumFileBlocks(fileSize int64) int {
	n := int(fileSize / MAX_BLOCK_SIZE)
	if fileSize%MAX_BLOCK_SIZE > 0 {
		n += 1
	}
	return n
}

func SplitFileIntoBlocks(filename string, outputDirectory string) bool {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return false
	}

	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return false
	}

	fileSize := info.Size()
	numBlocks := GetNumFileBlocks(fileSize)
	buffer := make([]byte, MAX_BLOCK_SIZE)

	for i := 0; i < numBlocks; i++ {
		n, err := f.Read(buffer)
		if err != nil {
			return false
		}

		outputFilename := fmt.Sprintf("%s/%s_block%d", outputDirectory, filename, i)
		outputFile, err := os.OpenFile(outputFilename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0640)
		if err != nil {
			return false
		}

		_, err = outputFile.Write(buffer)
		if err != nil {
			return false
		}

		outputFile.Close()

		if n < MAX_BLOCK_SIZE {
			break
		}
	}

	return true
}
