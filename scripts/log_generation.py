#!/usr/bin/env python3
import datetime
import random
import sys

test_pattern = {}

words = ["apple", "orange", "grape", "monkey", "mountain", "computer", "systems"]
value = 100
sum = 0
for word in words:
    test_pattern[word] = value
    sum += value
    value *= 5

print(test_pattern)


def inject_pattern(f):
    if len(test_pattern) > 0:
        choice = random.choice(list(test_pattern.keys()))
        test_pattern[choice] -= 1
        f.write(" " + str(choice))
        if test_pattern[choice] <= 0:
            del test_pattern[choice]


def main():
    random.seed(425)  # set fixed random seed
    with open(logfile, "w") as f:
        for i in range(round(sum * 1.5)):
            f.write(f"{datetime.datetime.now()} {random.randint(0, 9)}")
            inject_pattern(f)
            f.write("\n")
        print(test_pattern)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} filename")
        exit(1)

    logfile = sys.argv[1]
    main()
