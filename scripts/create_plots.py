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

latencies = []

for query in queries:
    trials = get_files(query)[DIRS]

    if len(trials) == 0:
        continue

    values = []
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
                    line_count += int(num_lines)
                    values.append(time_milli)

    num_hosts = len(values)

    if num_hosts == 0:
        continue

    # average line count
    line_count = round(line_count/num_hosts)
    print(line_count, values)

    latencies.append((np.log(values), line_count))


latencies.sort(key=lambda x: x[1])

for x in latencies: print(x[1])

latencies = list(map(lambda x: x[0], latencies))

# Create a boxplot
plt.boxplot(latencies)

# Add labels to the x-axis
trial_labels = [f"Trial {i+1}" for i in range(len(latencies))]
plt.xticks(range(1, len(latencies) + 1), trial_labels)

# Set the y-axis label
plt.ylabel('Values')

# Set the title
plt.title('Boxplot of Trial Data')

# Show the plot
plt.show()

# fig = plt.figure(figsize=(10, 7))
# ax = fig.add_axes([0, 0, 1, 1])
# bp = ax.boxplot(latencies)
#
# plt.show()
