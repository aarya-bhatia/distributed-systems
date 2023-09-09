#!/bin/sh

rm -rf outputs/ reports/
make clean
make

logfile="testlogs/log"

queries=("low" "mid" "high")

for query in "${queries[@]}"; do
	echo Query: $query
	for i in {1..5}; do
		echo Trial: $i
		bin/client -command "grep $query $logfile" -silence -reports "reports/$query/trial$i"
	done
done

