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

		// parts := strings.Split(strings.TrimSpace(line), ":")
		// if len(parts) == 2 {
		// 	key := parts[0]
		// 	value := parts[1]
		// 	res[key] = append(res[key], value)
		// }
	}

	return res
}

func ExecuteAndGetOutput(executable string, args []string, inputLines []string) ([]string, error) {
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
		for _, line := range inputLines {
			_, err := io.WriteString(stdinPipeWriter, line+"\n")
			if err != nil {
				log.Warn("Error writing to stdin pipe:", err)
				return
			}
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

	// // Create a pipe for the command's stdin
	// stdin, err := cmd.StdinPipe()
	// if err != nil {
	// 	log.Warn("Error creating StdinPipe:", err)
	// 	return nil, err
	// }
	//
	// // Create a pipe for the command's stdout
	// stdout, err := cmd.StdoutPipe()
	// if err != nil {
	// 	log.Warn("Error creating StdoutPipe:", err)
	// 	return nil, err
	// }
	//
	// log.Debug("Starting process...")
	//
	// // Start the command
	// if err := cmd.Start(); err != nil {
	// 	log.Warn("Error starting command:", err)
	// 	return nil, err
	// }
	//
	// log.Debug("sending input to process...")
	//
	// // Write input lines to the command's stdin
	// for _, line := range inputLines {
	// 	if _, err := fmt.Fprintln(stdin, line); err != nil {
	// 		log.Warn("Error writing to stdin:", err)
	// 		return nil, err
	// 	}
	// }
	//
	// stdin.Close() // Close the input pipe after writing input
	//
	// // Read the command's output
	// output := []string{}
	//
	// log.Debug("reading output from process...")
	//
	// reader := bufio.NewReader(stdout)
	// for {
	// 	line, err := reader.ReadString('\n')
	// 	if err != nil {
	// 		break
	// 	}
	//
	// 	if len(line) == 0 {
	// 		continue
	// 	}
	//
	// 	output = append(output, line[:len(line)-1])
	// }
	//
	// log.Debug("Waiting for process to exit...")
	//
	// // Wait for the command to finish
	// if err := cmd.Wait(); err != nil {
	// 	log.Warn("Error waiting for command:", err)
	// 	return nil, err
	// }
	//
	// log.Debug("all done")
	//
	// return output, nil
}
