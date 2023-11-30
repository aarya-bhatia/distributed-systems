#!/bin/sh
# ./all "echo KILL >/dev/udp/localhost/4000" # SDFS_FD_PORT
# ./all "echo KILL >/dev/udp/localhost/9000" # MAPLEJUICE_FD_PORT
# ./all "kill -9 \$(lsof -t -i:4000)"
./all "kill -9 \$(lsof -t -i:9000)"
