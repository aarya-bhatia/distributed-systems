#!/bin/sh
id=$1
if [ -z "$id" ]; then
	printf "Usage: $0 ID\n"
	exit 1
fi
tcpPort=$((5000+$id))
udpPort=$((6000+$id))
echo "Removing database files... db/$id"
rm -rf db/$id
echo "Creating new database directory... db/$id"
mkdir -p db/$id
command="go run . -h localhost -tcp $tcpPort -udp $udpPort -db db/$id"
echo $command
$command