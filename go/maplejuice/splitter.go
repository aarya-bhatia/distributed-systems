package maplejuice

import (
	"strings"
)

const BATCH_SIZE = 200

type LineInfo struct {
	Offset int
	Length int
}

func ProcessFileContents(fileContents string) []LineInfo {
	var lines []LineInfo // Array of structs to store line info and data

	offset := 0
	lineNum := 0

	// Split the file contents into lines
	fileLines := strings.Split(fileContents, "\n")

	// Read each line and store line content, offset, and length in the array of structs
	for _, line := range fileLines {
		lineInfo := LineInfo{
			Offset: offset,
			Length: len(line),
		}

		lines = append(lines, lineInfo)

		// Update offset for the next line
		offset += len(line) + 1 // Add 1 for newline character
		lineNum++
	}

	return lines
}

func GetLineGroups(lines []LineInfo, batchSize int) []LineInfo {
	batches := make([]LineInfo, 0)
	for len(lines) > 0 {
		batch := lines
		if len(lines) >= batchSize {
			batch = lines[:batchSize]
		}
		lines = lines[len(batch):]
		batchInfo := LineInfo{
			Offset: batch[0].Offset,
			Length: batch[len(batch)-1].Offset + batch[len(batch)-1].Length - batch[0].Offset,
		}
		batches = append(batches, batchInfo)
	}
	return batches
}
