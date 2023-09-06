#!/bin/bash

hosts_file="hosts"
results=()
score=0

CLIENT="dgrep"

test_pattern() {
	output_file=/tmp/output
	options=$1
	pattern=$2

	num_hosts=$(cat $hosts_file | grep -v '^!\|^\s*$' | wc -l)

	if [ ! -x $CLIENT ]; then
		/bin/go build go/client/client.go -o $CLIENT
	fi

	/bin/rm $output_file
	./dgrep $options "$pattern" | tee $output_file

	if [ ! $? -eq 0 ]; then
		echo client failed with status $?
		exit 1
	fi

	num_expected_matches=$(grep -c $options "$pattern" $input_file)
	num_actual_matches=$(grep -c $options "$pattern" $output_file)
	expected=$(printf "%s * %s\n" $num_hosts $num_expected_matches | bc)

	if [ $expected -eq $num_actual_matches ]; then
		results+=("Test passed for pattern \"$pattern\": matched $num_actual_matches out of $num_expected_matches lines from $num_hosts servers.")
		score=$((score+1))
	else
		results+=("Test failed for pattern \"$pattern\": matched $num_actual_matches out of $num_expected_matches lines from $num_hosts servers.")
		score=$((score-1))
	fi
}

patterns=("HTTP" "GET" "DELETE" "POST\|PUT" "[4-5]0[0-9]" "20[0-9]")
for pattern in "${patterns[@]}"; do
	test_pattern "-i" "$pattern"
done

echo "Results"
for result in "${results[@]}"; do
	echo "$result"
done

echo "Score: $score/${#patterns[@]}"


