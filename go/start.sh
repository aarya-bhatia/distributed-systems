#!/bin/sh
id=$1
if [ -z "$id" ]; then
	printf "Usage: $0 ID\n"
	exit 1
fi
tcpPort=$((5000+$id))
udpPort=$((6000+$id))
command="go run . -h localhost -tcp $tcpPort -udp $udpPort"
echo $command
$command
