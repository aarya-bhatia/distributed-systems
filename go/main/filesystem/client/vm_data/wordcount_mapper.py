#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if len(line) == 0:
        continue
    words = line.split()
    counts = {}
    for word in words:
        word = ''.join(filter(str.isalnum, word))
        if word not in counts:
            counts[word] = 0
        counts[word] += 1

    for word, count in counts.items():
        print(f"{word}:{count}")
