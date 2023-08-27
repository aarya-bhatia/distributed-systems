# MP1

## Server Architecture

Main thread:
- Listen for tcp connections on a specified port
- fork() a process for each connection and start worker

Worker process:
- Read the request from client socket
- Run the requested shell command in a new process
- Pipe the output of the command to the client socket

## Client Architecture

Main thread:
- Create a blocking message queue
- Loads the hosts ID, IP address and port from a file called "hosts"
- Create a thread for each host and open a tcp connection
- Blocks while message queue is empty
- When new message in queue, print it to stdout
- Wait for threads to exit

Worker thread:
- If cannot connect to host, exit worker
- Sends a request to the server containing the specified shell command
- Receives messages from the server and pushes them to message queue
- Messages are split by new line characters
- On completion, adds a FINISHED message to the queue



## TODO

- Refactor server to accept an ID (number) - use ID to create LOG file in directory:
    `/var/log/cs425/...`
- Ability to specify another log file to use for testing
- Add access logs on Server containing the client's request

