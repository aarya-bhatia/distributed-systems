#!/bin/bash

GIT_BRANCH=main
CS425_REPO=$HOME/cs425
MP1_LOGS=$HOME/mp1.log
MP1_PORT=3000
LOGS=$HOME/log
TCP_PORT=5000
UDP_PORT=6000

cd $HOME

rm -rf *.log $LOGS

mkdir -p .ssh/

[ -e ssh_cs425 ] && mv ssh_cs425 .ssh/cs425
[ -e ssh_cs425.pub ] && mv ssh_cs425.pub .ssh/cs425.pub
[ -e ssh_config ] && mv ssh_config .ssh/config

if [ ! -d $CS425_REPO ]; then
	git clone ssh://git@gitlab.engr.illinois.edu/aaryab2/cs425.git cs425
	if [ ! $? -eq 0 ]; then
		echo "Failed to clone repository"
		exit 1
	fi
fi

cd $CS425_REPO
git reset HEAD
git checkout $GIT_BRANCH || git checkout -b $GIT_BRANCH
git pull origin $GIT_BRANCH

# start shell server
if ! pgrep -f bin/server >/dev/null; then
	cd $CS425_REPO/mp1
	make
	pkill -f bin/server
	nohup bin/server $MP1_PORT >$MP1_LOGS 2>&1 &
	echo "shell server is running at $(hostname):$MP1_PORT$"
fi

# Restart filesystem and failure detector server
cd $CS425_REPO/go
if ! which nc >/dev/null; then
	pkill -f "go run"
else
	echo "KILL" | nc localhost $TCP_PORT
fi

# sleep 10 # wait for other vms to die
# go mod tidy
# nohup go run . >$LOGS 2>&1 &
# echo "SDFS server is running at $(hostname)"

