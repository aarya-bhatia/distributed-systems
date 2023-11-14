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
cd ./go/main/filesystem/server
go run .
```

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
cd ./go/main/filesystem/server
go run . <ID>
```

Notes:

- Id can be any number from 1 to 10
- Id 1 is the introducer
- Introducer must be alive for new nodes to join
- You can run commands on *stdin* (type "help")

## Client

```
cd go/main/filesystem/client
go run . <server_id> <command> <args>...

```

## Client Commands

- list file replicas: `ls <file>`
- download file from sdfs to disk: `get <remote> <local>`
- upload file from disk to sdfs: `put <local> <remote>`
- delete file from sdfs: `delete <remote>`

## Config

Config parameters can be changed in [here](./go/common/config.go)

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
