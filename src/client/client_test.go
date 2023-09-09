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
	queries := map[string]int{"low": 100, "mid": 10000, "high": 1000000}

	for pattern, count := range queries {

		client := RunClient(ClientArgs{
			command:         fmt.Sprintf("grep %s testlogs/log", pattern),
			outputDirectory: OUTPUT_PATH,
			logsDirectory:   "test_logs",
			silence:         true,
			grep:         	 "",
			hosts:			 HOSTS_FILE,
		})

		// client := RunClient(ClientArgs{
		// 	grep:            "grep " + pattern,
		// 	outputDirectory: OUTPUT_PATH,
		// 	logsDirectory:   "test_logs",
		// 	silence:         true,
		// 	command:         "",
		// })

		for _, host := range client.hosts {
			if host.status == STATUS_SUCCESS {
				if host.lines != count {
					t.Errorf("Test failed for pattern %s on host %s:%s", pattern, host.host, host.port)
				}
			}
		}
	}
}

func TestSampleData(t *testing.T) {
	var queries = []string{"HTTP", "GET", "DELETE", "POST\\|PUT", "[4-5]0[0-9]", "20[0-9]"}

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

				out, err := exec.Command("/bin/grep", "-c", pattern, filepath).Output()
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
