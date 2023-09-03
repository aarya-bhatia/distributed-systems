import datetime
import random

test_pattern = {"high frequency": 1e6, "mid frequency": 1e4, "low frequency": 1e2}

def inject_pattern(f):
    if len(test_pattern) > 0:
        # 80% chance to inject a pattern
        if random.randint(1, 10) <= 8:
            choice = random.choice(list(test_pattern.keys()))
            test_pattern[choice] -= 1
            f.write(" " + str(choice))
            if test_pattern[choice] <= 0:
                del test_pattern[choice]


def main():
    random.seed(425)
    with open("./log", "w") as f:
        for i in range(2000000):
            f.write(f"{datetime.datetime.now()} {random.randint(0, 9)}")
            inject_pattern(f)
            f.write("\n")
        print(test_pattern)

if __name__ == "__main__":
    main()
