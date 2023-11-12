# Simple distributed file system

## Build

- Prerequisite: go version 1.19
- Server: `cd go; go mod tidy; go build; ./cs425`
- Client: `cd go/client; go mod tidy; go build; ./client`

## Client usage

- Put/Upload file: `./client put localfilename remotefilename`
- Get/Download file: `./client get remotefilename localfilename`

## Terminology

- master = leader = name node - stores metadata and queues request
- slave = replica = data node - stores the file shards on disk

## Upload file protocol (client<->master)

- Client: UPLOAD_FILE name size

- Master: OK | ERROR
- Master: block replicas[,..]
- Master: ...
- Master: END

- Client: OK | ERROR
- Client: block replicas[,..]
- Client: ...
- Client: END

- Master: OK | ERROR

## Download file protocol (client<->master)

- Client: DOWNLOAD_FILE name

- Master: OK | ERROR
- Master: block size replicas[,..]
- Master: ...
- Master: END

- Client: OK

## Upload block protocol (client<->slave)

- Client: UPLOAD name size
- Slave: OK
- Client: Send data
- Slave: OK | ERROR

## Download block protocol (client<->slave)

- Client: DOWNLOAD name size
- Slave: Send data or disconnect


## Algorithm

### Uploading File

- C: send upload_file request to master node
- M: if request is invalid, return error
- M: enqueue request
- When master node dequeues the request:
- M: send OK
- M: send list of replicas
- For each block,
- C: send data to replicas, receive OK
- C: send master replicas where block was uploaded


