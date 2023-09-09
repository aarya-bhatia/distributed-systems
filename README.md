# MP1

## CS425 (Professor Indy)

Authors: Aarya Bhatia \<aaryab@illinois.edu\>, William Zheng \<xinzez2@illinois.edu\>

## Setup Instructions

- Install prerequisites: `g++`, `go`
- Build server and client: `make`
- Run the server: `bin/server <port>`
- Run the client: `bin/client args...`
- Help: `bin/client --help`
- Configure [./hosts](hosts) file to add or remove servers

## Client options
- The client can either run a grep command or any shell command:
    - To run a grep: `bin/client -grep "HTTP"` - You should only specify the
      query - not the program name ('grep') or file ('vm1.log') - those are
      automatically populated by the client, depending on the server.
    - To run a command: `bin/client -command "ls -la"` - This will list the
      files on each server relative to the root of the project. The output will
      be displayed on stdout. Use this with caution!
- To suppress the output: `bin/client -grep ... -silence`
- To change the path to server logs directory: `bin/client -logs data/`. You
  can change the path to the directory containing logs in the format
  `vm{i}.log`. This can be used to separate test log files from production log
  files.
- Each server's output will be saved to a file in the output directory
  ("outputs" by default) such as "outputs/vm1.output". You can change the
  output directory by passing the "-output" option.
- Each job will also create a report file containing host metadata. The reports
  path is `reports/<timestamp>`.
- To change the hosts config file you can use the `-hosts` option.


## Server Architecture

Main thread:
- Listens for tcp connections on given port
- Creates a new thread for each connection

Worker thread:
- Read request from client
- Run the requested shell command in a child process and capture output in
  parent process using a pipe
- Send the output of command to client


## Client Architecture

Main function:
- Parse the CLI arguments into a struct and initialize client
- Run the client program
- Display the metadata and stats to stdout

Client Main thread:
- Create a asynchronous blocking queue
- Loads the hosts from file into a list
- Creates thread for each host connection
- Creates the "output consumer" and "finished channel" threads
- Waits for all threads to join

- Output Consumer Routine:
    - Reads the lines from the queue and prints them to stdout.
    - Updates the client stats for total line and bytes received.
    - Thread quits when it receives the EXIT message.

- Finished Channel Routine:
    - Counts the number of finished threads.
    - It sends an EXIT message to the queue when all threads finish.

Worker thread:
- Attempt to connect to host or exit
- Send the request to server containing the grep command appended with server log file `{logsDirectory}/vm{i}.log`
- Reads output of command from the server and pushes each line to queue. Additionally, append lines to output file.
- Messages are read line by line
- Compute the latency of the response when connection is closed
- Save all meta data to the host struct - data transferred, line count, latency
- Signal the finished channel when returning from function

## Deploying

- Run the deploy script: `cd scripts && ./deploy.sh`
- Edit the [./scripts/start.sh](start.sh) script according to needs: `$EDITOR scripts/start.sh`
- The servers already have a copy of the Gitlab SSH key, ssh config file, the log file
- The start.sh scripts is copied to each host in cluster over an SSH connection
- The start.sh script is run on each server and clones/builds/starts server.
- To change the git branch or build targets, simply append the command to start.sh
