#!/bin/bash
x-terminal-emulator -e python metaserver.py 2222
sleep 2
x-terminal-emulator -e python dataserver.py 0 3333 4444 5555 6666
sleep 2
x-terminal-emulator -e python dataserver.py 1 3333 4444 5555 6666
sleep 2
x-terminal-emulator -e python dataserver.py 2 3333 4444 5555 6666
sleep 2
x-terminal-emulator -e python dataserver.py 3 3333 4444 5555 6666
sleep 2
python distributedFS.py fusemount 2222 3333 4444 5555 6666
