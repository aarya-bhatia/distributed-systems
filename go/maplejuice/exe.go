package maplejuice

import (
	"fmt"
	"regexp"
	"strings"
)

func WordCountMapper(lines []string) (map[string]int, error) {
	res := make(map[string]int)
	// Define a regular expression pattern to match special characters
	reg, err := regexp.Compile("[^a-zA-Z0-9\\s]+")
	if err != nil {
		fmt.Println("Error compiling regex:", err)
		return nil, err
	}

	for _, line := range lines {
		// Remove special characters from the line
		cleanedLine := reg.ReplaceAllString(line, " ")

		// Split the cleaned line into words
		words := strings.Fields(cleanedLine)

		for _, word := range words {
			res[word]++
		}
	}

	return res, nil
}
