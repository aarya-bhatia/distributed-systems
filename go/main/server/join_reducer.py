#!/usr/bin/env python3
import sys

# System args:
# arg 1: key

# User args:
# arg 2: file1
# arg 3: file2

if __name__ == "__main__":
    if len(sys.argv) < 4:
        exit(1)

    key = sys.argv[1]
    file1 = sys.argv[2]
    file2 = sys.argv[3]

    map = {file1: [], file2: []}

    for line in sys.stdin:
        line = line.strip()
        tokens = line.split(',')

        if len(line) == 0 or len(tokens) == 0:
            continue

        filename = tokens[0]

        if filename == file1 or filename == file2:
            map[filename].append(line)

    for line1 in map[file1]:
        for line2 in map[file2]:
            print(f"{key}:{line1},{line2}")
