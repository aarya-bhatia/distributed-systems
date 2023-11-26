#!/bin/sh
echo "sending map job..."
go run . maple wordcount_mapper.py 1 prefix sample
sleep 3
echo "sending reduce job..."
go run . juice wordcount_reducer.py 1 prefix output
