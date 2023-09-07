# MP1

## Setup Instructions

- Prerequisites: Install `g++` and `go`
- Build: `make`
- Run the server: `bin/server <id> <port>`
- Run the client: `bin/client ...args`
- Help: `go/client/client --help`
- Configure: Update the hosts file to add or remove servers (id, hostname, port)

## Server Architecture

Main thread:
- Listens for tcp connections on given port
- Creates a new thread for each connection

Worker thread:
- Read request from client
- Add log for client request
- Run the requested shell command in a child process and capture output in
  parent process using a pipe a pipe.
- Write the output of command to client

## Client Architecture

Main thread:
- Create a asynchronous queue which blocks while empty
- Loads the hosts from file
- Creates thread for each host and connect to server
- Another thread to listen for messages on queue and print/write to
  stdout/file. Thread quits when it receives the message "EXIT".
- Creates a "finished" channel that counts how many threads have finished. It
  sends a "EXIT" message to queue when all threads finish.
- Main thread waits for all threads to finish

Worker thread:
- If cannot connect to host, exit worker
- Writes the request to server containing a grep command and the input file
  (eg. `vm{i}.log`)
- Reads output of command from the server and pushes each line to queue
- Messages are split by new line characters
- Compute the latency of the response when connection is closed
- Save all meta data to the host struct (data transferred, line count, latency)
- Inform finished channel when finished, on both success and failure

