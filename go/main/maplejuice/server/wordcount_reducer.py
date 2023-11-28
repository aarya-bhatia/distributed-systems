#!/usr/bin/env python3
import sys

count = 0

key = sys.argv[1]

for line in sys.stdin:
    line = line.strip()
    if len(line) == 0:
        continue
    count += int(line)

print(key + ":" + str(count))
