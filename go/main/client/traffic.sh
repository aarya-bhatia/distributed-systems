#!/bin/bash
# map <output> <input>
# juice <input> <output>

param=Fiber

if [ ! -z $1 ]; then
	param=$1
fi

map_exe1=maplejuice_exe/demo_map1
map_exe2=maplejuice_exe/demo_map2
reduce_exe1=maplejuice_exe/demo_reduce1
reduce_exe2=maplejuice_exe/demo_reduce2

echo "uploading exectuables..."
go run . 1 put ${map_exe1} ${map_exe1}
go run . 1 put ${map_exe2} ${map_exe2}
go run . 1 put ${reduce_exe1} ${reduce_exe1}
go run . 1 put ${reduce_exe2} ${reduce_exe2}

filename="data/traffic.csv"
echo "uploading input file..."
go run . 1 put $filename $filename

echo "sending map job 1 (input=traffic, output=prefix1)"
go run . 1 maple ${map_exe1} 1 prefix1 ${filename} ${param}

echo "sending reduce job 1 (input=prefix1, output=output1)"
go run . 1 juice ${reduce_exe1} 1 prefix1 output1

echo "sending map job 2 (input=output1, output=prefix2)"
go run . 1 maple ${map_exe2} 1 prefix2 output1

echo "sending reduce job 2 (input=prefix2, output=output2)"
go run . 1 juice ${reduce_exe2} 1 prefix2 output2

