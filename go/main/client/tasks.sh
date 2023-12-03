#!/bin/sh
go run . 1 rmdir /

go run . 1 put data/sample sample
go run . 1 put data/traffic.csv traffic

# go run . 1 put data/vm1 vm1
# go run . 1 put data/test1 test1
# go run . 1 put data/test2 test2

# go run . 1 put data/orders.csv dataset/orders.csv
# go run . 1 put data/customers.csv dataset/customers.csv

exe_dir=../../maplejuice/maplejuice_exe

for file in $(ls ${exe_dir}/*.py ${exe_dir}/demo*); do
	go run . 1 put $file $(basename $file)
done
