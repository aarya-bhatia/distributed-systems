# mp2

## Build Instructions

1. [Install Go](https://go.dev/doc/install)
2. Run `make` from the mp2 directory
3. Usage: `./main --help`

**Example:** To start server on port with log level INFO and Gossip+S mode: `./main -h <hostname> -p <port> -l 'info' -s`

NOTE: All VMs run on port 6000 and the first VM is the introducer.

**Example:** To start local server in Gossip mode: `./main -h "localhost" -p <port>`

NOTE: The local introducer server must be run on port 6001.

## Commands

The server can accept the following UDP messages:

1. To list the members: `ls`
2. To kill the server: `kill`
3. To stop gossip: `stop_gossip`
4. To start server: `start_gossip`
5. Toggle suspicion protocol: `sus ON`, `sus OFF`
6. To list all commands: `help`

**Example:** To use netcat: `echo ls | nc -w1 -u fa23-cs425-0701.cs.illinois.edu 6000`

