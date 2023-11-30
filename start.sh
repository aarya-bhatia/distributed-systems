#!/bin/bash
GIT_BRANCH=main
SHELL_PORT=3000
SDFS_FD_PORT=4000
MAPLEJUICE_FD_PORT=9000

# clean logs
cd $HOME
rm -rf log *.log *.out *.dat data/

# update repo
cd $HOME/cs425/go
git reset --hard HEAD
git checkout $GIT_BRANCH || git checkout -b $GIT_BRANCH
git pull origin $GIT_BRANCH
go mod tidy

# start shell server, if not running
if ! fuser ${SHELL_PORT}/tcp; then
	cd $HOME/cs425/go/shell/server
	nohup go run . >$HOME/mp1.log 2>&1 &
fi

# stop sdfs server
if fuser ${SDFS_FD_PORT}/udp; then
	echo KILL >/dev/udp/localhost/${SDFS_FD_PORT}
fi

# stop maplejuice server
if fuser ${MAPLEJUICE_FD_PORT}/udp; then
	echo KILL >/dev/udp/localhost/${MAPLEJUICE_FD_PORT}
fi

# start sdfs
cd $HOME/cs425/go/main/filesystem/server
nohup go run . >$HOME/mp3.log 2>&1 &

# start maplejuice
# cd $HOME/cs425/go/main/maplejuice/server
# nohup go run . >$HOME/mp4.log 2>&1 &

echo node $(hostname) is online

