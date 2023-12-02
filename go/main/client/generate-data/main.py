import csv
import os
import random
from datetime import datetime, timedelta
import humanize

num_customers = 5e4
num_orders = 1e5
num_products = 2e3

customers_file = "../data/customers.csv"
orders_file = "../data/orders.csv"


def get_random_ip():
    return ".".join((str(random.randint(0, 255)) for _ in range(4)))


def generate_customers(rows_to_generate):
    with open(customers_file, 'w', newline='') as csvfile:
        fieldnames = ['id', 'name', 'email', 'gender', 'ip_address']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, rows_to_generate + 1):
            writer.writerow({
                'id': i,
                'name': f'user{i}',
                'email': f'user{i}@example.com',
                'gender': 'Male' if i % 2 == 0 else 'Female',
                'ip_address': get_random_ip(),
            })


def generate_random_dates(start, end, count):
    date_list = []
    for _ in range(count):
        random_date = start + \
            timedelta(days=random.randint(0, (end - start).days))
        date_list.append(random_date.strftime('%Y-%m-%d'))
    return sorted(date_list)


def generate_orders(num_customers, rows_to_generate):
    with open(orders_file, 'w', newline='') as csvfile:
        fieldnames = ['order_id', 'customer_id',
                      'product_id', 'order_date', 'price', 'status']

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        start_date = datetime(1969, 1, 1)
        end_date = datetime.now()
        random_dates = generate_random_dates(
            start_date, end_date, rows_to_generate)

        for i in range(rows_to_generate):
            writer.writerow({
                'order_id': i + 1,
                'customer_id': random.randrange(1, num_customers),
                'product_id': 1 + i % num_products,
                'order_date': random_dates[i],
                'price': random.randrange(99, 9999),
                'status': random.choice(['Processing', 'Shipping', 'Delivered'])
            })


if __name__ == "__main__":
    print(
        f"Generating data for {num_customers} customers to {customers_file}...")
    generate_customers(int(num_customers))

    print(f"Generating data for {num_orders} orders to {orders_file}...")
    generate_orders(int(num_customers), int(num_orders))

    print(f"Size of {customers_file}:", humanize.naturalsize(
        os.stat(customers_file).st_size, binary=True))
    print(f"Size of {orders_file}:", humanize.naturalsize(
        os.stat(orders_file).st_size, binary=True))
