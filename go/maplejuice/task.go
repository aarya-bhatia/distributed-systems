package maplejuice

import (
	"bufio"
	"cs425/filesystem/client"
	"io"
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
		line = strings.TrimSpace(line)
		keySeparator := strings.Index(line, ":")
		if keySeparator < 0 {
			continue
		}
		key := line[:keySeparator]
		value := line[keySeparator+1:]
		res[key] = append(res[key], value)
	}

	return res
}

func ExecuteAndGetOutput(executable string, args []string, inputLines string) ([]string, error) {
	cmd := exec.Command(executable, args...)

	stdinPipeReader, stdinPipeWriter := io.Pipe()
	stdoutPipeReader, stdoutPipeWriter := io.Pipe()

	cmd.Stdin = stdinPipeReader
	cmd.Stdout = stdoutPipeWriter

	// Start the command
	err := cmd.Start()
	if err != nil {
		log.Warn("Error starting command:", err)
		return nil, err
	}

	// Write input lines to the stdin pipe of the subprocess
	go func() {
		defer stdinPipeWriter.Close()
		r := strings.NewReader(inputLines)
		if _, err := io.Copy(stdinPipeWriter, r); err != nil {
			log.Warn("Error writing to stdin pipe:", err)
			return
		}
	}()

	outputLines := []string{}

	// Read output lines from the stdout pipe of the subprocess
	go func() {
		defer stdoutPipeReader.Close()
		scanner := bufio.NewScanner(stdoutPipeReader)
		for scanner.Scan() {
			outputLine := scanner.Text()
			outputLines = append(outputLines, outputLine)
		}
		if err := scanner.Err(); err != nil {
			log.Warn("Error reading from stdout pipe:", err)
			return
		}
	}()

	// Wait for the command to finish
	err = cmd.Wait()
	if err != nil {
		log.Warn("Command execution error:", err)
		return nil, err
	}

	return outputLines, nil
}
