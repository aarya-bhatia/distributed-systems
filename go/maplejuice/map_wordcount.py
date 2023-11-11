try:
    while True:
        line = input()
        for word in line.strip().split():
            print(word, 1)
except EOFError:
    pass
