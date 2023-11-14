#!/bin/bash

PASSWD_FILE="/home/aarya/passwd"
NETID="aaryab2"
HOSTS="hosts"

if [ ! -f $PASSWD_FILE ]; then
	echo Please save your netid password in a file and update the PASSWD_FILE \
		variable in this script.
	exit 1
fi

timeout=20

for vm in $(cat $HOSTS); do
	echo "VM: $vm"
	ping -W $timeout -c 1 $vm

	if [ $? -eq 0 ]; then
		sshpass -f $PASSWD_FILE rsync -avu -e "ssh -o 'StrictHostKeyChecking no'" start.sh $NETID@$vm:~/start.sh
		sshpass -f $PASSWD_FILE ssh -f -n -o "StrictHostKeyChecking no" $NETID@$vm "./start.sh" &
	else
		echo "Failed to connect to $vm"
	fi
done

