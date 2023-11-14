#!/bin/sh
echo $@ | go run . "echo KILL >/dev/udp/localhost/4000"

