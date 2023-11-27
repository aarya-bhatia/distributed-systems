#!/bin/sh
go run . 1 rmdir /
go run . 1 put data/sample sample
go run . 1 put data/vm1 vm1

exe_dir=./maplejuice_exe
for file in $(ls ${exe_dir}/*.py); do
	go run . 1 put $file $(basename $file)
done

