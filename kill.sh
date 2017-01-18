#!/bin/bash
# Assuming no other Python process is running
#killall -9 python 
ports=( 2222 3333 4444 5555 6666 )
for port in "${ports[@]}"
do
  lsof -i tcp:${port} | awk 'NR!=1 {print $2}' | xargs kill 
done
