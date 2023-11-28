#!/bin/sh
go run . maple wordcount_mapper.py 1 prefix sample
go run . juice wordcount_reducer.py 1 prefix output
