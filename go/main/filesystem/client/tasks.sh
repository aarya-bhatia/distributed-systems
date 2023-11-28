#!/bin/sh
go run . 1 rmdir /
go run . 1 put data/sample sample
go run . 1 put data/vm1 vm1
go run . 1 put data/orders.csv dataset/orders.csv
go run . 1 put data/customers.csv dataset/customers.csv

go run . 1 put data/test1 test1
go run . 1 put data/test2 test2

exe_dir=./maplejuice_exe
for file in $(ls ${exe_dir}/*.py); do
	go run . 1 put $file $(basename $file)
done

