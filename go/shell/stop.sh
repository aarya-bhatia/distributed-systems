#!/bin/sh
echo $@ | go run . "echo KILL | nc localhost 5000"
