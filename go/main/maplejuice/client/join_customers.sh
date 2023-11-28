#!/bin/sh
go run . maple join_mapper.py 2 prefix dataset dataset/customers.csv 0 dataset/orders.csv 1
go run . juice join_reducer.py 2 prefix output dataset/customers.csv dataset/orders.csv
