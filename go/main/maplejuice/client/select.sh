#!/bin/bash
# {radio, fiber, fiber/radio, none}

if [[ $# -lt 2 ]]; then
	echo "Usage: $0 column regex"
	exit 1
fi

column=$1
regex=$2

echo "sending map job"
cmd="go run . maple demo_sql_map 1 prefix traffic $column $regex"
echo $cmd && $cmd

echo "sending reduce job"
cmd="go run . juice demo_sql_reduce 1 prefix output"
echo $cmd && $cmd
