#!/usr/bin/env python3

import os
import matplotlib.pyplot as plt
import numpy as np

results = []

REPORTS_DIR = "../reports"

FILES = 0
DIRS = 1


def get_files(path):
    files = []
    dirs = []
    if os.path.exists(path):
        for entry in os.listdir(path):
            entry_path = os.path.join(path, entry)
            if os.path.isdir(entry_path):
                dirs.append(entry_path)
            elif os.path.isfile(entry_path):
                files.append(entry_path)

    return files, dirs


queries = get_files(REPORTS_DIR)[DIRS]

for query in queries:
    trials = get_files(query)[DIRS]

    if len(trials) == 0:
        continue

    latencies = []
    line_count = 0

    for trial in trials:
        if "trial" in trial:
            reports = get_files(trial)[FILES]
            if len(reports) > 0:
                lines = open(reports[0]).readlines()
                for line in lines:
                    tokens = line.split(",")
                    num_lines = tokens[-3]
                    time_nano = tokens[-2]
                    time_milli = float(time_nano) / 1e6
                    latencies.append(time_milli)
                    line_count += int(num_lines)

    num_hosts = len(latencies)

    if num_hosts == 0:
        continue

    # average line count
    line_count = round(line_count/num_hosts)

    average_latency = np.mean(latencies)
    std_latency = np.std(latencies)

    print(line_count, average_latency, std_latency)
