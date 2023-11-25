#!/bin/sh
echo "sending map job..."
go run . maple wordcount_mapper.py 1 prefix sample
echo "sending reduce job..."
go run . juice wordcount_reducer.py 1 prefix output
