#!/bin/bash
go run . 1 cat $1 $1
which batcat 2>/dev/null && batcat $1 || less $1
wc $1
