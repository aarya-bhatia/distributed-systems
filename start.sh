#!/bin/bash
GIT_BRANCH=main

cd $HOME
rm -rf log *.log *.out *.dat data/

cd $HOME/cs425/go
git reset --hard HEAD
git checkout $GIT_BRANCH || git checkout -b $GIT_BRANCH
git pull origin $GIT_BRANCH
go mod tidy

if pgrep -f 'go run' >/dev/null; then
	echo KILL >/dev/tcp/localhost/3000
	echo KILL >/dev/udp/localhost/4000
	echo KILL >/dev/udp/localhost/9000
fi

cd $HOME/cs425/go/main/filesystem/server
nohup go run . >$HOME/mp3.log 2>&1 &

cd $HOME/cs425/go/shell/server
nohup go run . >$HOME/mp1.log 2>&1 &

cd $HOME/cs425/go/main/maplejuice/server
nohup go run . >$HOME/mp4.log 2>&1 &

echo node $(hostname) is online

