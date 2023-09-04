#!/bin/bash

passwd_file="passwd"

if [ ! -e $passwd_file ]; then
	echo "Password file not found"
	exit 1
fi

cluster=(
	"fa23-cs425-0701.cs.illinois.edu"
	"fa23-cs425-0702.cs.illinois.edu"
	"fa23-cs425-0703.cs.illinois.edu"
	"fa23-cs425-0704.cs.illinois.edu"
	"fa23-cs425-0705.cs.illinois.edu"
	"fa23-cs425-0706.cs.illinois.edu"
	"fa23-cs425-0707.cs.illinois.edu"
	"fa23-cs425-0708.cs.illinois.edu"
	"fa23-cs425-0709.cs.illinois.edu"
	"fa23-cs425-0710.cs.illinois.edu"
)

timeout=3

for vm in "${cluster[@]}"; do
	echo "vm: $vm"
	ping -W $timeout -c 1 $vm

	if [ $? -eq 0 ]; then
		sshpass -f $passwd_file scp ~/.ssh/cs425 ~/.ssh/cs425.pub ./start.sh ./ssh_config $vm:~
		sshpass -f $passwd_file ssh $vm "mkdir -p ~/.ssh; mv ~/cs425 ~/cs425.pub ~/.ssh/; mv ~/ssh_config ~/.ssh/config; /bin/bash ~/start.sh"
	else
		echo "Failed to connect to $vm"
	fi
done

