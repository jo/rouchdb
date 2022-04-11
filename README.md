# RouchDB
Minimal CouchDB on Rust.

Dream: have a Rust database which can replicate to and from a Couch. No MR. Optimized for small docs with small attachments.

First Steps:
1. Write a HTTP database adapter
2. Write a replicator
3. Write a LevelDB database adapter


## State of Work

**Very** basic replication works \o/

### TODO
* use session id
* compare replication logs
* request changes since
* store replication logs
* make it work with arbritary docs (currently only foo is supported)
* make it work with attachments
* emit errors
* rething struct naming

(c) 2022 by Johannes J. Schmidt
