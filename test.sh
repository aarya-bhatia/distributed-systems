#!/bin/bash

hosts_file="hosts"
output_file=/tmp/output
client="go/client/client"

sh -c "cd go/client; go build"

num_hosts=$(cat $hosts_file | grep -v '^!\|^\s*$' | wc -l)

results=()
score=0

input_file=data/all.log
[ ! -f $input_file ] && find data/ -type f | xargs cat >$input_file

test_pattern() {
	pattern="$1"
	/bin/rm $output_file
	$client "$pattern" | tee $output_file

	if [ ! $? -eq 0 ]; then
		results+=("Test failed for pattern \"$pattern\": client failed with status $?")
	else
		num_expected_matches=$(grep -c "$pattern" $input_file)
		num_actual_matches=$(grep -c "$pattern" $output_file)

		if [ $num_expected_matches -eq $num_actual_matches ]; then
			results+=("Test passed for pattern \"$pattern\": matched $num_actual_matches out of $num_expected_matches lines from $num_hosts servers.")
			score=$((score+1))
		else
			results+=("Test failed for pattern \"$pattern\": matched $num_actual_matches out of $num_expected_matches lines from $num_hosts servers.")
			score=$((score-1))
		fi
	fi
}

queries=("-i http" "GET" "DELETE" "POST\|PUT" "[4-5]0[0-9]" "20[0-9]")
for query in "${queries[@]}"; do
	test_pattern "$query"
done

echo "Results"
for result in "${results[@]}"; do
	echo "$result"
done

echo "Score: $score/${#queries[@]}"


