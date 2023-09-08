package main

import "testing"
import "fmt"
import "os/exec"

const OUTPUT_PATH = "../../outputs"

func TestPattern(t *testing.T) {
	queries := map[string]int{"low": 100, "mid": 10000, "high": 1000000}

	for pattern, count := range queries {

		client := RunClient(ClientArgs{
			grep:            "grep " + pattern,
			outputDirectory: OUTPUT_PATH,
			logsDirectory:   "test_logs",
			silence:         true,
			command:         "",
		})

		for _, host := range client.hosts {
			if host.status == STATUS_SUCCESS {
				if host.lines != count {
					t.Fail()
				}
			}
		}

		fmt.Println(client.stat)
	}
}

func TestSampleData(t *testing.T) {
	for _, pattern := range []string{"DELETE", "HTTP"} {

		client := RunClient(ClientArgs{
			grep:            "grep " + pattern,
			outputDirectory: OUTPUT_PATH,
			logsDirectory:   "data",
			silence:         true,
			command:         "",
		})

		for _, host := range client.hosts {
			if host.status == STATUS_SUCCESS {
				cmd := fmt.Sprintf("grep -c %s data/vm%s.log", pattern, host.id)
				fmt.Println("Command: ", cmd)
				out, err := exec.Command(cmd).Output()
				if err != nil {
					log.Fatal(err)
				}

				expected := int(strings.TrimSpace(out))

				if host.lines != count {
					t.Fail()
				}
			}
		}

		fmt.Println(client.stat)
	}
}
