#!/bin/bash
# {radio, fiber, fiber/radio, none}

filename=data/traffic.csv
map_exe=maplejuice_exe/select_map
reduce_exe=maplejuice_exe/select_reduce

if [[ $# -lt 2 ]]; then
	echo "Usage: $0 column regex"
	exit 1
fi

column=$1
regex=$2

go run . 1 put ${map_exe} ${map_exe}
go run . 1 put ${reduce_exe} ${reduce_exe}
go run . 1 put ${filename} ${filename}
go run . 1 maple ${map_exe} 1 prefix ${filename} ${column} ${regex}
go run . 1 juice ${reduce_exe} 1 prefix output

