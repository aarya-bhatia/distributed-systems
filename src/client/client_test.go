package main

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const HOSTS_FILE = "../../hosts"
const OUTPUT_PATH = "../../outputs"

func TestPattern(t *testing.T) {
	words := []string {"apple","orange","grape","monkey","mountain","computer","systems"}
	lines := 100

	for _, word := range words {

		client := RunClient(ClientArgs{
			command:         fmt.Sprintf("grep %s testlogs/log", word),
			grep:         	 "",
			outputDirectory: OUTPUT_PATH,
			silence:         true,
			hosts:			 HOSTS_FILE,
		})

		for _, host := range client.hosts {
			if host.status == STATUS_SUCCESS {
				if host.lines != lines {
					t.Errorf("Test failed for pattern %s on host %s:%s", word, host.host, host.port)
				}
			}
		}

		lines *= 5
	}
}

func TestSampleData(t *testing.T) {
	queries := []string{"HTTP", "GET", "DELETE", "POST\\|PUT", "[4-5]0[0-9]", "20[0-9]", "-i -P get\\s.*mozilla"}

	for _, pattern := range queries {
		client := RunClient(ClientArgs{
			grep:            "grep " + pattern,
			outputDirectory: OUTPUT_PATH,
			logsDirectory:   "data",
			silence:         true,
			command:         "",
 			hosts:			 HOSTS_FILE,
		})

		for _, host := range client.hosts {
			if host.status == STATUS_SUCCESS {
				filepath, err := filepath.Abs(fmt.Sprintf("../../data/vm%s.log", host.id))
				if err != nil {
					t.Fatalf("Failed to resolve filepath: %v", err)
				}

				tokens := strings.Split(pattern, " ")
				tokens = append([]string{"/bin/grep","-c"},append(tokens,filepath)...)
				t.Log(tokens)

				out, err := exec.Command(tokens[0], tokens[1:]...).Output()
				if err != nil {
					t.Fatalf("Failed to execute command: %v", err)
				}

				expected, err := strconv.Atoi(strings.TrimSpace(string(out)))
				if err != nil {
					t.Fatalf("Failed to parse output: %v", err)
				}

				if host.lines != expected {
					t.Errorf("Test failed for pattern %s (Expected: %d, Actual: %d)", pattern, expected, host.lines)
				}
			}
		}
	}
}
