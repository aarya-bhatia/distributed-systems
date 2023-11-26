#!/bin/sh
go run . 1 rmdir /
go run . 1 put data/sample sample
go run . 1 put data/vm1 vm1

for file in $(ls *.py); do
	go run . 1 put $file $file
done

