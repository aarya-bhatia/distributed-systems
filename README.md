# CS425

## Introduction

1. Author: Aarya Bhatia (aaryab2@illinois.edu)
2. MP3 Report: ./MP3_Report.pdf
3. VM addresses: ./hosts

## Build Instructions

1. [Install Go v1.19](https://go.dev/doc/install)

## Start all VMs

- Power on all vms from https://vc.cs.illinois.edu
- Run `./deploy.sh`

## Stop all VMs

```
cd go/shell
./stopall.sh
```

## Tail all log files on VMs

```
cd go/shell
./watch.sh
```

**NOTE**: To restart server, please run stop first.

## Run locally

```
cd go
go run . <ID>
```

Notes:

- Id can be any number from 1 to 10
- Id 1 is the introducer
- Introducer must be alive for new nodes to join
- You can run commands on *stdin* (type "help")

## Client

```
cd go/client
```
- The client can run commands from a "tasks" file, or stdin.
- Example: `cat tasks | go run .` or `go run . <tasks`
- The format of a task file is: `<VM> <command> <args>...`,
where VM can be any number from 1 to 10. Possible commands are "get", "put", "ls", "delete".

Examples:
- `echo 1 put /home/aaryab2/file1 file1 | go run . ` will upload file1 from VM1 to SDFS
- `echo 2 get file1 /home/aaryab2/file1.out | go run .` will download file1 from SDFS to VM2
- You can add a "sleep x" command to space out the commands by 'x' seconds.
- Any blank line or lines starting with '#' are ignored.

