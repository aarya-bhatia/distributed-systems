# MP1

## Server Architecture

Main thread:
- Listen for tcp connections
- fork() a process for each connection

Child process:
- Read the request from client
- fork() and run the grep program
- Pipe the output to the socket

## Client Architecture

Main thread:
- Creates a thread for each machine and opens tcp connections
- Blocks while queue is empty
- When new message in queue, print it to stdout

Worker thread:
- Sends the request to the server containing the options and query term
- Receives the logs from the server and pushes them to queue


