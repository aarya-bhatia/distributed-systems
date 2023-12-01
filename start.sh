#!/bin/bash
GIT_BRANCH=main
SHELL_PORT=3000
FDPORT=4000

# clean logs
cd $HOME
rm -rf log *.log *.out *.dat data/

# update repo
cd $HOME/cs425/go
git stash
git checkout $GIT_BRANCH || git checkout -b $GIT_BRANCH
git pull origin $GIT_BRANCH
go mod tidy

# start shell server if not running
if ! fuser ${SHELL_PORT}/tcp; then
	cd $HOME/cs425/go/shell/server
	nohup go run . >$HOME/mp1.log 2>&1 & echo "started shell server"
fi

# restart sdfs and maplejuice server
if fuser ${FDPORT}/udp; then
	kill -9 \$(lsof -t -i:${FDPORT})
	echo "node killed"
fi

cd $HOME/cs425/go/main/server
nohup go run . >$HOME/log 2>&1 &
echo node $(hostname) is online

