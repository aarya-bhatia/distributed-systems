#!/bin/bash

GIT_BRANCH=frontend-server

CS425_REPO=$HOME/cs425

MP1_LOGS=$HOME/mp1.log
MP1_PORT=3000
LOGS=$HOME/log
TCP_PORT=5000

cd $HOME
rm -rf log *.log *.out data

cd $CS425_REPO/go

git reset HEAD
git checkout $GIT_BRANCH || git checkout -b $GIT_BRANCH
git pull origin $GIT_BRANCH

go mod tidy

if ! pgrep -f server >/dev/null; then
	nohup go run cs425/shell/server $MP1_PORT >$MP1_LOGS 2>&1 &
fi

if pgrep -f cs425 >/dev/null; then
	echo KILL | nc localhost $TCP_PORT
fi

sleep 5 # wait for other vms to die

nohup go run . >$LOGS 2>&1 &

