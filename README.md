# CS425

## Introduction

1. Authors: Aarya Bhatia (aaryab2@illinois.edu), William Zheng
2. [MP3 Report](./MP3_Report.pdf)
3. [VM addresses](./hosts)

## Build Instructions

1. [Install Go v1.19](https://go.dev/doc/install)

## Start all VMs

- Power on all vms from https://vc.cs.illinois.edu
- Run `./deploy.sh`

To start each server manually on VMs:

```
cd go/main/server
go run .
```

## Stop all VMs

```
cd go/shell
./stopall.sh
```

## Run locally

```
cd ./go/main/server
go run . <ID>
```

Notes:

- ID can be any number from 1 to 10
- You can run commands on *stdin* (type "help")
- Introducer must be alive for new nodes to join

## Start Introducer

- Development

```
cd go/main/failuredetector
go run . localhost 34000
```

- Production: Login to vm1 and run the following:

```
cd go/main/failuredetector
go run . $(hostname) 34000
```

## Client

```
cd go/main/client
go run . <server_id> <command> <args>...

```

## Client Commands

- to print usage: Run without any args
- To list file replicas: `ls <file>`
- To download file from sdfs to disk: `get <remote> <local>`
- To upload file from disk to sdfs: `put <local> <remote>`
- To delete file from sdfs: `delete <remote>`
- To append file: `append <local> <remote>`
- To concatenate files in directory (prefix): `cat <prefix> <output>`
- To list files in directory: `lsdir <directory>`
- To remove directory: `rmdir <directory>`
- To list all files: `lsdir /`
- To delete all files: `rmdir /`
- Run map command (see usage): `maple ...`
- Run reduce command (see usage): `juice ...`

## Server Commands

These commands can be sent by stdin to SDFS servers.

- `kill`: crash server
- `list_mem`: print FD membership table
- `list_self`: print FD member id
- `join`: start gossiping
- `leave`: stop gossiping
- `info`: Display node info
- `store`: Display local files blocks
- `leader`: Print leader node
- `files`: Print file metadata
- `queue`: Print file queues status

## Config

Config parameters can be changed in [here](./go/common/config.go)

