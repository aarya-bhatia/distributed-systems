#!/bin/sh
go run . 1 put ./maplejuice_exe/wordcount_mapper.py wordcount_mapper
go run . 1 put ./maplejuice_exe/wordcount_reducer.py wordcount_reducer

go run . 1 maple wordcount_mapper 1 prefix sample
go run . 1 juice wordcount_reducer 1 prefix output
