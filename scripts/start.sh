#!/bin/bash

cd $HOME

mkdir -p .ssh/

[ -e ssh_cs425 ] && mv ssh_cs425 .ssh/cs425
[ -e ssh_cs425.pub ] && mv ssh_cs425.pub .ssh/cs425.pub
[ -e ssh_config ] && mv ssh_config .ssh/config

if [ ! -d mp1 ]; then
	git clone ssh://git@gitlab.engr.illinois.edu/aaryab2/mp1.git mp1
	if [ ! $? -eq 0 ]; then
		echo "Failed to clone repository"
		exit 1
	fi
fi

cd mp1
git checkout main
git pull origin main
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

pkill -f bin/server
nohup bin/server $port 2>&1 >stdout &
echo "Server $id running at $host:$port"

