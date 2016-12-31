#!/bin/bash

cd fusemount
mkdir dir1
ls
mkdir dir2
ls
mkdir dir3
ls
cd dir1
mkdir dir10
ls
mkdir st_mode
ls
cd ..
echo " Hey.This is a test script for the project" > f1.txt
ls
cat f1.txt
#tree
echo " Hello there" > f2.txt
ls

cd ..
python serverterminate.py
sleep 2
printf "Server down : server 4444 crashed/n" 
cd fusemount
printf "Printing contents of f1.txt and f2.txt with server 4444 down"
cat f1.txt
cat f2.txt
#tree
printf "Reading occurs after server is down also\n"

echo "Hi I am creating a third file and trying to writethis text to it\n" >f3.txt
printf "Write is blocked until server restarts\n"
cd ..
sleep 2
x-terminal-emulator -e python dataserver.py 1 2222 3333 4444 5555 6666
printf "Server restarted and restores contents . Write to f3.txt completes\n "
sleep 2
cd fusemount
printf "checking if the write to f3.txt completed. Printing f3.txt: \n"
cat f3.txt
printf "printing contents of f2.txt:\n"
cat f2.txt
rmdir dir3
ls
mv f1.txt newfile.txt
ls
cat newfile.txt
mv newfile.txt dir1/
ls
cd dir1
cat newfile.txt
cd ..
ls
#tree
cd ..
printf "Calling corrupt function\n"
python corrupt.py
cd fusemount
printf "printing contents of f2.txt f3.txt: \n"
cat f2.txt
cat f3.txt

