# MP1

## Setup Instructions

- Compile the code: `make`
- Run the server: `bin/server <id> <port>`
- Run the client: `bin/client <command> [...args]`
- Create log directory: `sudo mkdir -p /var/log/cs425 && sudo chown $USER:$USER /var/log/cs425`
- Add hosts to file "hosts" with the format: `Server ID, IP Address, Port`

## Server Architecture

Main thread:
- Initializes file logger thread
- Listens for tcp connections on a specified port
- Creates a thread for each connection

File logger:
- Opens the log file using server ID `machine.<ID>.log`
- Reads messages from queue and blocks while empty
- Appends a log message to file

Worker process:
- Read the request from client socket
- Add log for the request made by the client
- Add log for establishing connection
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

