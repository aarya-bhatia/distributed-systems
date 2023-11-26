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


Sat Nov 18 08:35:38 AM CST 2023

- maplejuice balance nummaples across worker nodes
- testing
- add reduce job


Sun Nov 26 12:37:50 AM CST 2023

- send tasks in chunks for better performance
- check issue with invalid file size in reduce job?
- download executables in worker, not executor
- test worker failure and fix issues
- try different mappers and reducers, with large dataset
- test on VM
