#!/bin/bash

if [[ $# -lt 2 ]]; then
	echo "Usage: $0 filename regex"
	exit 1
fi

filename=$1
regex=$2

go run . maple filter_mapper.py 1 prefix $filename $regex
go run . juice filter_reducer.py 1 prefix output

