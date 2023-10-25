#!/bin/sh
tcpPort=5000
udpPort=6000
echo "Removing database files... db/$id"
rm -rf db/$id
echo "Creating new database directory... db/$id"
mkdir -p db/$id
command="go run . -h $(hostname) -tcp $tcpPort -udp $udpPort -db db"
echo $command
$command

