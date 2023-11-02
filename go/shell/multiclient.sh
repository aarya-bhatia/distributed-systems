#!/bin/bash

# Default values
readNodes=""
writeNodes=""
filename="file"

print_usage=false

# Function to display usage information
usage() {
	echo "Usage: $0 -r=nodes -w=nodes -f=filename"
	echo "  -r: Read nodes"
	echo "  -w: Write nodes"
	echo "  -f: Filename"
	echo "Example: $0 -r=1,2 -w=3 -f=file"
	echo "This will initiate a read from nodes 1,2, a write from node 3, for the file with given name."
	exit 1
}

# Loop through all the arguments
while [[ $# -gt 0 ]]; do
	case "$1" in
		-r=*)
			readNodes="${1#-r=}"
			readNodes=$(echo "$readNodes" | tr ',' ' ')  # Convert comma-separated to space-separated
			shift
			;;
		-w=*)
			writeNodes="${1#-w=}"
			writeNodes=$(echo "$writeNodes" | tr ',' ' ')  # Convert comma-separated to space-separated
			shift
			;;
		-f=*)
			filename="${1#-f=}"
			filename=$(echo "\$HOME/$filename")
			shift
			;;
		--usage)
			print_usage=true
			shift
			;;
		--help)
			print_usage=true
			shift
			;;
		*)
			echo "Unknown option: $1"
			usage
			;;
	esac
done

if $print_usage; then
	usage
fi

echo "readNodes: $readNodes"
echo "writeNodes: $writeNodes"
echo "filename: $filename"

h="/home/aaryab2"

if [ -z ${readNodes} ]; then
	echo ${readNodes} | go run . ${h}/client get ${filename} ${h}/${filename}.out &
fi

if [ -z ${writeNodes} ]; then
	echo ${writeNodes} | go run . ${h}/client put ${h}/${filename} ${filename} &
fi

echo "running"
