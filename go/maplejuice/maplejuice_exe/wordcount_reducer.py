#!/usr/bin/env python3
import sys

count = 0

for line in sys.stdin:
    line = line.strip()
    count += int(line)

print(count)
