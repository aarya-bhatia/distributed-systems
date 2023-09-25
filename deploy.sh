#!/bin/bash

passwd_file="/home/aarya/passwd"
netid="aaryab2"

if [ ! -f $passwd_file ]; then
	echo Password file not found: $passwd_file
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
	echo "VM: $vm"
	ping -W $timeout -c 1 $vm

	if [ $? -eq 0 ]; then
		sshpass -f $passwd_file rsync -avu -e "ssh -o 'StrictHostKeyChecking no'" start.sh $netid@$vm:~/start.sh
		sshpass -f $passwd_file ssh -f -n -o "StrictHostKeyChecking no" $netid@$vm "./start.sh" &
	else
		echo "Failed to connect to $vm"
	fi

	sleep 2
done

