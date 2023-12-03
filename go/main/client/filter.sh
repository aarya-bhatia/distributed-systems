#!/bin/bash

if [[ $# -lt 2 ]]; then
	echo "Usage: $0 filename regex"
	exit 1
fi

filename=$1
regex=$2

if [ ! -f $filename ]; then
	echo File does not exist: $filename
	exit 1
fi

map_exe=maplejuice_exe/filter_mapper.py
reduce_exe=maplejuice_exe/filter_reducer.py

# upload executables
go run . 1 put ${map_exe} ${map_exe}
go run . 1 put ${reduce_exe} ${reduce_exe}

# upload input file
go run . 1 put ${filename} ${filename}

# run jobs
go run . 1 maple ${map_exe} 4 prefix ${filename} $regex
go run . 1 juice ${reduce_exe} 1 prefix output

