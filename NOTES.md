# TODO
- sdfs server
    - replicate and re-balance blocks
    - append file
- file server
    - append block
- sdfs client
    - append file

- maplejuice
    - reduce job, reduce task
    - use sdfs client at leader to download and send blocks to workers
    - worker use sdfs client to append data to file (prefix\_key)

- sdfs server:
    - client heartbeat service

- client - caching on retry

