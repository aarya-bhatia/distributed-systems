#!/bin/bash
# map <output> <input>
# juice <input> <output>

param=Fiber

if [ ! -z $1 ]; then
	param=$1
fi

echo "sending map job 1 (input=traffic, output=prefix1)"
cmd="go run . maple demo_map1 1 prefix1 traffic ${param}"
echo $cmd && $cmd

echo "sending reduce job 1 (input=prefix1, output=output1)"
cmd="go run . juice demo_reduce1 1 prefix1 output1"
echo $cmd && $cmd

echo "sending map job 2 (input=output1, output=prefix2)"
cmd="go run . maple demo_map2 1 prefix2 output1"
echo $cmd && $cmd

echo "sending reduce job 2 (input=prefix2, output=output2)"
cmd="go run . juice demo_reduce2 1 prefix2 output2"
echo $cmd && $cmd

echo "done."
