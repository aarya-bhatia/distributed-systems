#!/usr/bin/env python3

import sys

d = {}

for line in sys.stdin:
    line = line.strip()
    key, value = line.split(":")
    if not key in d:
        d[key] = 0
    d[key] += int(value)

for key, value in d.items():
    print(f"{key}:{value}")
