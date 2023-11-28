#!/bin/sh
# go run . maple join_mapper.py 2 prefix dataset dataset/customers.csv 0 dataset/orders.csv 1
# sleep 2
# go run . juice join_reducer.py 2 prefix output dataset/customers.csv dataset/orders.csv
go run . maple join_mapper.py 1 prefix test test1 0 test2 0
sleep 2
go run . juice join_reducer.py 1 prefix output test1 test2
