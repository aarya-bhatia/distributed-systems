#!/bin/sh
# go run . maple filter_mapper.py 1 prefix sample a
# go run . juice filter_reducer.py 1 prefix output

go run . maple filter_mapper.py 1 prefix vm1 DELETE
go run . juice filter_reducer.py 1 prefix output

