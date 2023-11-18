package maplejuice

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
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

func WordCountReducer(lines []string) (map[string]int, error) {
	res := make(map[string]int)

	for _, line := range lines {
		tokens := strings.Split(line, ":")
		if len(tokens) != 2 {
			continue
		}
		key := tokens[0]
		value, err := strconv.Atoi(tokens[1])
		if err != nil {
			log.Println(err)
			continue
		}
		res[key] += value
	}

	return res, nil
}
