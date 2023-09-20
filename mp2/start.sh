#!/bin/bash

cd $HOME

mkdir -p .ssh/

[ -e ssh_cs425 ] && mv ssh_cs425 .ssh/cs425
[ -e ssh_cs425.pub ] && mv ssh_cs425.pub .ssh/cs425.pub
[ -e ssh_config ] && mv ssh_config .ssh/config

if [ ! -d cs425 ]; then
	git clone ssh://git@gitlab.engr.illinois.edu/aaryab2/cs425.git cs425
	if [ ! $? -eq 0 ]; then
		echo "Failed to clone repository"
		exit 1
	fi
fi

rm -rf mp1; # old repo

cd cs425
git checkout main
git pull origin main

cd mp2
make clean
make

line=$(cat hosts | grep -m 1 -i "$(hostname)")

if [ ! -z "$line" ]; then
	id=$(echo "$line" | cut -d' ' -f1)
	host=$(echo "$line" | cut -d' ' -f2)
	port=$(echo "$line" | cut -d' ' -f3)
fi

if [ -z $id ] || [ -z $host ] || [ -z $port ]; then
	echo "Failed to read server information"
	exit 1
fi

echo $id, $host, $port
pkill -f ./main
nohup ./main $host $port >log 2>&1 &
echo "Server $id running at $host:$port"

# pkill -f bin/server
# nohup bin/server $port >stdout 2>&1 &


