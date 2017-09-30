# Distributed filesystem in user space

## Usage
This is essentially a Distributed FileSystem in userspace with fault tolerance/redundancy written over Python. To start the system, invoke ./run.sh - it will start a metaserver and 4 dataservers. Your filesystem will be mounted at a directory called 'fusemount' at the same space where you ran the system. Create all files and directories as you wish in that space. To exit the system, close the terminal where you ran run.sh and call ./kill.sh to kill all the servers. Presently only Linux has been supported.
