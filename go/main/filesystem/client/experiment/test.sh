#!/bin/sh
port=4001

rm -rf *.out

testfile() {
	echo "Uploading files..."
	filename=$1
	remote=$(echo $filename | tr '/' '_')
	fileout=${remote}.out

	echo $filename, $remote, $fileout

	read -p "enter to start upload" ans
	echo put $filename $remote | nc -q 1 localhost $port

	read -p "upload done. enter to start download" ans
	echo get $remote $fileout | nc -q 1 localhost $port

	read -p "download done. enter to start compare" ans
	if [ -f $fileout ]; then
		diff $filename $fileout || echo "file is different"
	fi

	echo "Cleaning up..."
	rm -rf $fileout
}

testfile data/small
testfile data/mid
testfile data/large

