package maplejuice

import (
	"bufio"
	"cs425/filesystem/client"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Task interface {
	Run(sdfsClient *client.SDFSClient) (map[string][]string, error)
	Hash() int
	GetID() int64
	GetExecutable() string
}

func CleanInputLines(lines []string) error {
	// To match non-alphanumeric characters
	reg, err := regexp.Compile("[^a-zA-Z0-9\\s]+")
	if err != nil {
		log.Warn("Error compiling regex:", err)
		return err
	}

	// Clean input lines
	for i, line := range lines {
		lines[i] = reg.ReplaceAllString(line, " ")
	}

	return nil
}

func ParseKeyValuePairs(output []string) map[string][]string {
	res := make(map[string][]string)

	for _, line := range output {
		parts := strings.Split(strings.TrimSpace(line), ":")
		if len(parts) == 2 {
			key := parts[0]
			value := parts[1]
			res[key] = append(res[key], value)
		}
	}

	return res
}

func ExecuteAndGetOutput(executable string, args []string, inputLines []string) ([]string, error) {
	// Initialize the command
	cmd := exec.Command(executable, args...)

	// Create a pipe for the command's stdin
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Println("Error creating StdinPipe:", err)
		return nil, err
	}

	// Create a pipe for the command's stdout
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Println("Error creating StdoutPipe:", err)
		return nil, err
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		log.Println("Error starting command:", err)
		return nil, err
	}

	// Write input lines to the command's stdin
	for _, line := range inputLines {
		if _, err := fmt.Fprintln(stdin, line); err != nil {
			log.Println("Error writing to stdin:", err)
			return nil, err
		}
	}
	stdin.Close() // Close the input pipe after writing input

	// Read the command's output
	output := []string{}

	reader := bufio.NewReader(stdout)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		if len(line) == 0 {
			continue
		}

		output = append(output, line[:len(line)-1])
	}

	// Wait for the command to finish
	if err := cmd.Wait(); err != nil {
		log.Println("Error waiting for command:", err)
		return nil, err
	}

	return output, nil
}
