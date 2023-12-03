# generate data for many-many join

import csv
import os
import random
from datetime import datetime, timedelta
import humanize
import sys

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} num_rows")
    exit(1)

n = int(sys.argv[1])

num_customers = n
num_orders = n
num_products = 1000
num_ip = 5

customers_file = f"../data/customers_{n}.csv"
orders_file = f"../data/orders_{n}.csv"

ips = []


def get_random_ip():
    third = random.randint(0, 255)
    return f"192.168.{third}.1"


for i in range(num_ip):
    ips.append(get_random_ip())


def generate_customers(rows_to_generate):
    with open(customers_file, 'w', newline='') as csvfile:
        fieldnames = ['name', 'gender', 'ip_address']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, rows_to_generate + 1):
            customer = {
                # 'id': i,
                'name': f'user{i}',
                # 'email': f'user{i}@example.com',
                'gender': 'Male' if i % 2 == 0 else 'Female',
                'ip_address': random.choice(ips),
            }
            writer.writerow(customer)


def generate_random_dates(start, end, count):
    date_list = []
    for _ in range(count):
        random_date = start + \
            timedelta(days=random.randint(0, (end - start).days))
        date_list.append(random_date.strftime('%Y-%m-%d'))
    return sorted(date_list)


def generate_orders(rows_to_generate):
    with open(orders_file, 'w', newline='') as csvfile:
        fieldnames = ['order_id', 'order_date', 'status', 'ip_address']

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        start_date = datetime(1969, 1, 1)
        end_date = datetime.now()
        random_dates = generate_random_dates(
            start_date, end_date, rows_to_generate)

        for i in range(rows_to_generate):
            writer.writerow({
                'order_id': i + 1,
                # 'customer_id': customer["id"],
                # 'product_id': 1 + i % num_products,
                'order_date': random_dates[i],
                # 'price': random.randrange(99, 9999),
                'status': random.choice(['Processing', 'Shipping', 'Delivered']),
                'ip_address': random.choice(ips),
            })


if __name__ == "__main__":
    print(
        f"Generating data for {num_customers} customers to {customers_file}...")
    generate_customers(int(num_customers))

    print(f"Generating data for {num_orders} orders to {orders_file}...")
    generate_orders(int(num_orders))

    print(f"Size of {customers_file}:", humanize.naturalsize(
        os.stat(customers_file).st_size, binary=True))
    print(f"Size of {orders_file}:", humanize.naturalsize(
        os.stat(orders_file).st_size, binary=True))
