package maplejuice

import (
	"strings"
)

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
