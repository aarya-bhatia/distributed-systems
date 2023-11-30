#!/bin/sh
go run . 1 rmdir /

# go run . 1 put data/sample sample
# go run . 1 put data/vm1 vm1
# go run . 1 put data/orders.csv dataset/orders.csv
# go run . 1 put data/customers.csv dataset/customers.csv
# go run . 1 put data/test1 test1
# go run . 1 put data/test2 test2
go run . 1 put data/traffic.csv traffic

go run . 1 put ./maplejuice_exe/demo_map1 demo_map1
go run . 1 put ./maplejuice_exe/demo_map2 demo_map2
go run . 1 put ./maplejuice_exe/demo_reduce1 demo_reduce1
go run . 1 put ./maplejuice_exe/demo_reduce2 demo_reduce2
go run . 1 put ./maplejuice_exe/demo_sql_map demo_sql_map
go run . 1 put ./maplejuice_exe/demo_sql_reduce demo_sql_reduce

# exe_dir=./maplejuice_exe
# for file in $(ls ${exe_dir}/*.py ${exe_dir}/demo*); do
# 	go run . 1 put $file $(basename $file)
# done
