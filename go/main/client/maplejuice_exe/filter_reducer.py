#!/usr/bin/env python3
import sys

# arg 1: key
# identity reducer
if __name__ == "__main__":
    if len(sys.argv) < 2:
        exit(1)

    key = sys.argv[1]

    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue
        print(f"{key}:{line}")
