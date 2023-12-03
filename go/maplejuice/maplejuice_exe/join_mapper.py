#!/usr/bin/env python3
import sys

# System args:
# arg 1: current filename

# User args:
# arg 2: file1
# arg 3: column1
# arg 4: file2
# arg 5: column2

# Goal:
# To join file1.column1 with file2.column2

# Input: A valid csv file

# Output:
# The key is either file1.column1 or file2.column2
# Key: filename <other fields...>


def run(filename, column):
    for line in sys.stdin:
        line = line.strip()
        tokens = line.split(',')

        if column >= len(tokens):
            continue

        print(f"{tokens[column]}:{filename},{line}")


if __name__ == "__main__":
    if len(sys.argv) < 6:
        exit(1)

    filename = sys.argv[1]

    file1 = sys.argv[2]
    col1 = int(sys.argv[3])
    file2 = sys.argv[4]
    col2 = int(sys.argv[5])

    if filename == file1:
        run(filename, col1)
    elif filename == file2:
        run(filename, col2)
    else:
        exit(1)
