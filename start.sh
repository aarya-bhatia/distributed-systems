#!/bin/bash

if [ $# -lt 3 ]; then
	printf "Usage: %s ID PORT\n" $0
	exit 1
fi

ID=$1
PORT=$2

cd $HOME

if [ ! -e mp1 ]; then
	git clone ssh://git@gitlab.engr.illinois.edu/aaryab2/mp1.git
	if [ ! $? -eq 0 ]; then
		echo "failed to clone repository"
		exit 1
	fi
fi

cd mp1
git pull origin main
make clean
make

pkill server
nohup bin/server $ID $PORT 2>&1 > "$hostname.out" &


