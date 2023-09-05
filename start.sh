#!/bin/bash

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

if [ ! -e "$HOME/mp1" ]; then
	git clone ssh://git@gitlab.engr.illinois.edu/aaryab2/mp1.git $HOME/mp1
	if [ ! $? -eq 0 ]; then
		echo "Failed to clone repository"
		exit 1
	fi
fi

cd ~/mp1
git pull
make clean
make
pkill server
nohup bin/server $id $port 2>&1 > "$hostname.out" &

