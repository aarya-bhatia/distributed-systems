#!/usr/bin/env python3

import sys
import re

# arg 1: filename
# arg 2: regex
if __name__ == "__main__":
    if len(sys.argv) < 3:
        exit(1)

    filename = sys.argv[1]
    reg = sys.argv[2]

    for line in sys.stdin:
        line = line.strip()
        if re.search(reg, line, re.IGNORECASE) != None:
            print(f"{filename}:{line}")
