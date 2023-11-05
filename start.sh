#!/bin/bash
GIT_BRANCH=main

cd $HOME
rm -rf log *.log *.out data

cd $HOME/cs425/go

git reset HEAD
git checkout $GIT_BRANCH || git checkout -b $GIT_BRANCH
git pull origin $GIT_BRANCH
go mod tidy

if pgrep -f cs425 >/dev/null; then
	echo KILL | nc localhost 5000
	echo KILL | nc localhost 3000
fi

nohup go run . >$HOME/log 2>&1 &

cd shell/server
nohup go run . >$HOME/mp1.log 2>&1 &

echo node $(hostname) is online

