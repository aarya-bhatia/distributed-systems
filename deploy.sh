#!/bin/bash

passwd_file="../passwd"
netid="aaryab2"

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
		sshpass -f $passwd_file scp -v -o 'StrictHostKeyChecking no' ~/.ssh/cs425 $netid@$vm:~/ssh_cs425
		sshpass -f $passwd_file scp -v -o 'StrictHostKeyChecking no' ~/.ssh/cs425.pub $netid@$vm:~/ssh_cs425.pub
		sshpass -f $passwd_file scp -v -o 'StrictHostKeyChecking no' ssh_config $netid@$vm:~/ssh_config
		sshpass -f $passwd_file scp -v -o 'StrictHostKeyChecking no' start.sh $netid@$vm:~/start.sh
		sshpass -f $passwd_file ssh -f -n -o 'StrictHostKeyChecking no' $netid@$vm "./start.sh"
	else
		echo "Failed to connect to $vm"
	fi
done

for i in {0..9}; do
	file="vm$((i+1)).log"
	vm=${cluster[i]}
	echo "vm: $vm"
	sshpass -f $passwd_file scp -o 'StrictHostKeyChecking no' data/$file $netid@$vm:~
	sshpass -f $passwd_file ssh -o 'StrictHostKeyChecking no' $netid@$vm "mkdir -p ~/mp1/data; mv $file ~/mp1/data"
done
