#!/usr/bin/env python3

import sys
import re

# take 1 arg for regex
# if we pass in worker id we can make that the key
# therefore we can distribute lines evenly
# since filter has identity reduce
if __name__ == "__main__":
    if len(sys.argv) < 2:
        exit(1)

    reg = sys.argv[1]
    for line in sys.stdin:
        line = line.strip()
        if re.search(reg, line) != None:
            print(f"{line}:1")
