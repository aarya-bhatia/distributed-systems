#!/bin/bash

pattern=$1

if [ -z $pattern ]; then
	printf 'Usage: %s pattern\n' $0
	exit 1
fi

num_hosts=$(cat hosts | grep -v '^!\|^\s*$' | wc -l)

echo Testing pattern "$pattern" with $num_hosts hosts.
echo Input file: log
echo Output file: output

go run go/client/client.go grep "$pattern" log | tee output

if [ ! $? -eq 0 ]; then
	echo client failed with status $?
	exit 1
fi

num_expected_matches=$(grep "$pattern" log | wc -l)
num_actual_matches=$(grep "$pattern" output | wc -l)
expected=$(printf "%s * %s\n" $num_hosts $num_expected_matches | bc -q)

echo $num_actual_matches, $num_expected_matches, $expected

if [ $expected -eq $num_actual_matches ]; then
	echo 'Test passed :)!'
else
	echo 'Test failed :(!'
fi


