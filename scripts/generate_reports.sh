#!/bin/sh

rm -rf outputs/ reports/
make

logfile="testlogs/log"
queries=("apple" "orange" "grape" "monkey" "mountain" "computer" "systems")

for query in "${queries[@]}"; do
	echo Query: $query
	for i in {1..5}; do
		echo Trial: $i
		grep -v "^$\|^!" hosts | shuf -n 4 | tee /tmp/hosts$i
		bin/client -hosts /tmp/hosts$i -command "grep $query $logfile" -silence -reports "reports/$query/trial$i"
	done
done

