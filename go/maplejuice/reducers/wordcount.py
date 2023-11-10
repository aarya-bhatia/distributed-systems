key = input().strip()
count = 0

try:
    while True:
        count += int(input().strip())
except EOFError:
    pass

print(key,count)
