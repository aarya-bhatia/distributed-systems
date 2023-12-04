#!/bin/sh
# ./all "tail -f ~/*log"
./all "[ -f ~/introducer.log ] && tail -f ~/introducer.log"
