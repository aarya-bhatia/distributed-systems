#!/bin/sh

go run . 1 rmdir /

go run . 1 put vm_data/sample sample
go run . 1 put vm_data/orders.csv dataset/orders.csv
go run . 1 put vm_data/customers.csv dataset/customers.csv
go run . 1 put vm_data/test1 test1
go run . 1 put vm_data/test2 test2
go run . 1 put vm_data/traffic.csv traffic

go run . 1 put ~/vm1 vm1

exe_dir=/home/aaryab2/cs425/go/maplejuice/maplejuice_exe

for file in $(ls ${exe_dir}/*.py ${exe_dir}/demo*); do
	go run . 1 put $file $(basename $file)
done
