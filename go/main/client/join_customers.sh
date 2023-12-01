#!/bin/sh
go run . 1 maple join_mapper.py 2 prefix dataset dataset/customers.csv 0 dataset/orders.csv 1
go run . 1 juice join_reducer.py 2 prefix output dataset/customers.csv dataset/orders.csv
