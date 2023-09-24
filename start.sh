#!/bin/bash

GIT_BRANCH=main

MP1_LOGS=$HOME/mp1.log
MP2_LOGS=$HOME/mp2.log

MP1_PORT=3000
MP2_PORT=6000

CS425_REPO=$HOME/cs425

cd $HOME

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
git checkout $GIT_BRANCH || git checkout -b $GIT_BRANCH
git pull origin $GIT_BRANCH

cd $CS425_REPO/mp1
make clean; make
pkill -f bin/server
nohup bin/server $MP1_PORT >$MP1_LOGS 2>&1 &
echo "MP1 Server is running at $(hostname):$MP1_PORT$"

cd $CS425_REPO/mp2
make clean; make
pkill -f ./main
nohup ./main -p $MP2_PORT >$MP2_LOGS 2>&1 &
echo "MP2 Server is running at $(hostname):$MP2_PORT$"

