#!/bin/sh
# map <output> <input>
# juice <input> <output>
go run . maple demo_map1 1 prefix1 traffic Fiber
go run . juice demo_reduce1 1 prefix1 output1
go run . maple demo_map2 1 prefix2 output1
go run . juice demo_reduce2 1 prefix2 output2
