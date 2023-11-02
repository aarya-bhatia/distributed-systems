#!/bin/bash
GIT_BRANCH=main
CS425_REPO=$HOME/cs425
MP1_LOGS=$HOME/mp1.log
LOGS=$HOME/log

cd $HOME
rm -rf log *.log *.out data

cd $CS425_REPO/go
git reset HEAD
git checkout $GIT_BRANCH || git checkout -b $GIT_BRANCH
git pull origin $GIT_BRANCH

if pgrep -f cs425 >/dev/null; then
	echo KILL | nc localhost 5000 2>/dev/null
	echo KILL | nc localhost 3000 2>/dev/null
fi

go mod tidy
nohup go run cs425/shell/server $MP1_PORT >$MP1_LOGS 2>&1 &
sleep 5
nohup go run cs425 >$LOGS 2>&1 &

