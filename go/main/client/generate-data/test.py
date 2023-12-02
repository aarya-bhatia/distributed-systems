import re

date_pattern = '[1-9][0-9]{3}-[0-9]{1,2}-[0-9]{1,2}'
ip_pattern = '[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}'

matches = 0

with open('orders.csv', 'r') as file:
    file.readline()
    for line in file:
        if re.search(date_pattern, line) != None:
            matches += 1
        else:
            print("failed match:", line)

    print(file.name, date_pattern, matches)

matches = 0

with open('customers.csv', 'r') as file:
    file.readline()
    for line in file:
        if re.search(ip_pattern, line) != None:
            matches += 1
        else:
            print("failed match:", line)

    print(file.name, ip_pattern, matches)
