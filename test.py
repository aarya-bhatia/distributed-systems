import os
HOST_DIR = "./hosts"
TEST_LOG_DIR = "./logs/log"
CLIENT_DIR = "./go/client/client.go"
GO_RUN_CMD = "go run"
LOW_FREQUENCY_CNT = int(1e2)
MEDIUM_FREQUENCY_CNT = int(1e4)
HIGH_FREQUENCY_CNT = int(1e6)

hosts = {}
def read_host():
    with open(HOST_DIR, "r") as f:
        for line in f.readlines():
            words = line.split(" ")
            hosts[words[0]] = {"host": words[1], "port": words[2], "low": 0, "mid": 0, "high": 0}
    print("Host loaded:")
    print(hosts)

def test_frequency(frequency: str, expected_frequency):
    os.system(f"{GO_RUN_CMD} {CLIENT_DIR} {frequency} {TEST_LOG_DIR} > {frequency}.log")
    with open(f"{frequency}.log", "r") as f:
        for line in f:
            if "---Meta data---" in line:
                break
            words = line.split(" ")
            id = words[0]
            assert(id in hosts.keys())
            if frequency in line:
                hosts[id][frequency] += 1
        
    for id in hosts.keys():
        assert hosts[id][frequency] == expected_frequency, f"Expect {expected_frequency} but got {hosts[id][frequency]} at id: {id}, host: {hosts[id]['host']} port: {hosts[id]['port']}"
    print(f"Passed {frequency} frequency test")

read_host()
test_frequency("low", LOW_FREQUENCY_CNT)
test_frequency("mid", MEDIUM_FREQUENCY_CNT)
test_frequency("high", HIGH_FREQUENCY_CNT)

