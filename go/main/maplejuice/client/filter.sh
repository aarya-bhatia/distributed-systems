#!/bin/sh
echo "sending map job..."
go run . maple filter_mapper.py 1 prefix sample a
echo "sending reduce job..."
go run . juice filter_reducer.py 1 prefix output
