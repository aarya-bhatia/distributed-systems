package main

import "testing"
import "fmt"

const TEST_LOG_PATH = "../../test_logs"
const OUTPUT_PATH = "../../outputs"

func TestPattern(t *testing.T) {
	queries := map[string]int{"low": 100, "mid": 10000, "high": 1000000}

	for pattern, count := range queries {

		client := RunClient(ClientArgs{
			grep:            "grep " + pattern,
			outputDirectory: OUTPUT_PATH,
			logsDirectory:   TEST_LOG_PATH,
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
