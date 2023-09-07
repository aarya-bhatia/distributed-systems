#!/bin/bash

hosts_file="hosts"
client="bin/client"

make bin/client

results=()
score=0

[ ! -f data/all.log ] && find data/ -type f | xargs cat >$input_file

num_hosts=$(cat $hosts_file | grep -v '^!\|^\s*$' | wc -l)

test_grep() {
	output_file=/tmp/cs425.out
	pattern="$1"
	logs=$2
	input_file=$3

	/bin/rm $output_file

	$client -grep "$pattern" -output $output_file -silence -logs $logs

	if [ ! $? -eq 0 ]; then
		results+=("Test failed for pattern \"$pattern\": client failed with status $?")
	else
		num_expected_matches=$(grep -c "$pattern" $input_file)
		num_actual_matches=$(grep -c "$pattern" $output_file)

		echo $num_actual_matches, $num_expected_matches

		if [ $num_expected_matches -eq $num_actual_matches ]; then
			results+=("Test passed for pattern \"$pattern\": matched $num_actual_matches out of $num_expected_matches lines from $num_hosts servers.")
			score=$((score+1))
		else
			results+=("Test failed for pattern \"$pattern\": matched $num_actual_matches out of $num_expected_matches lines from $num_hosts servers.")
		fi
	fi
}

test_num=$1

if [ -z $test_num ] || [ $test_num -eq 0 ]; then
	echo "Test 0"
	queries=("-i http" "GET" "DELETE" "POST\|PUT" "[4-5]0[0-9]" "20[0-9]")
	for query in "${queries[@]}"; do
		test_grep "$query" data data/all.log
	done
elif [ $test_num -eq 1 ]; then
	echo "Test 1"
	queries=("low" "mid" "high")
	for query in "${queries[@]}"; do
		test_grep "$query" logs logs/vm1.log
	done
fi

echo "Results"
for result in "${results[@]}"; do
	echo "$result"
done

echo "Score: $score/${#queries[@]}"


