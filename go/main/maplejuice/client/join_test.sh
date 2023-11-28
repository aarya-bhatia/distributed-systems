#!/bin/sh
go run . maple join_mapper.py 1 prefix test test1 0 test2 0
go run . juice join_reducer.py 1 prefix output test1 test2
