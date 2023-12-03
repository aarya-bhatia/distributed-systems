#!/bin/sh

go run . 1 rmdir /

go run . 1 put data/customers_200.csv data/customers_200.csv
go run . 1 put data/orders_200.csv data/orders_200.csv

go run . 1 put ./maplejuice_exe/join_mapper.py join_mapper.py
go run . 1 put ./maplejuice_exe/join_reducer.py join_reducer.py

go run . 1 maple join_mapper.py 4 prefix data data/customers_200.csv 0 data/orders_200.csv 1
go run . 1 juice join_reducer.py 4 prefix output data/customers_200.csv data/orders_200.csv

./cat.sh output 2>/dev/null
wc output

# go run . 1 maple join_mapper.py 4 prefix data data/customers_400.csv 0 data/orders_400.csv 1
# go run . 1 juice join_reducer.py 4 prefix output data/customers_400.csv data/orders_400.csv
#
# go run . 1 maple join_mapper.py 4 prefix data data/customers_600.csv 0 data/orders_600.csv 1
# go run . 1 juice join_reducer.py 4 prefix output data/customers_600.csv data/orders_600.csv
#
# go run . 1 maple join_mapper.py 4 prefix data data/customers_800.csv 0 data/orders_800.csv 1
# go run . 1 juice join_reducer.py 4 prefix output data/customers_800.csv data/orders_800.csv
#
# go run . 1 maple join_mapper.py 4 prefix data data/customers_1000.csv 0 data/orders_1000.csv 1
# go run . 1 juice join_reducer.py 4 prefix output data/customers_1000.csv data/orders_1000.csv
