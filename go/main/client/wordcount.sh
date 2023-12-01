#!/bin/sh
go run . 1 maple wordcount_mapper.py 1 prefix sample
go run . 1 juice wordcount_reducer.py 1 prefix output
