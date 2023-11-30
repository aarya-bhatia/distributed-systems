#!/bin/bash

echo "sending map job"
go run . maple demo_sql_map 1 prefix traffic "$1" "$2"
echo "sending reduce job"
go run . juice demo_sql_reduce 1 prefix output
