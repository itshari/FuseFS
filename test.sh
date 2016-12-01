#!/bin/bash

cd fusemount
mkdir dir1
ls
mkdir dir2
ls
mkdir dir3
ls
cd dir1
mkdir dir11
ls
mkdir dir12
ls
cd ..
echo " Hello. This is a testscript for the assignment." > file1
ls
cat file1
cd ..
python corrupt.py
cd fusemount
cat file1
echo " Right." > file2
ls
cd ..
#python Terminate.py
sleep 2
cd fusemount
cat file2
cat file1
cd ..
#x-terminal-emulator -e python dataserver.py 21234 11234 31234
printf "Server restarting....restoring contents"
sleep 2
cd fusemount
cat file1
cat file2
cd dir2
ls
echo "Alright" > file3
ls
cat file3
cd ..
ls
rmdir dir3
ls
mv file1 newfile
ls
cat newfile
mv newfile dir1
ls
cd dir1
ls
cat newfile
cd ..
ls
mv dir1 dir2
ls
cd dir2
ls
cd dir1
ls
echo "Okay Fine" > filen
ls
cat filen
rm filen
ls
rm newfile
ls
cd dir11
ls
mkdir dir123
ls
cd ..
cd dir11
cd ..
rmdir dir12
ls
rmdir dir11
ls
cd ..
#ln -s /home/ajay/fusepy/fusemount/dir2/dir1 newdir
ls
cd newdir
ls
echo "Okay" > filex
ls
cat filex
cd ..
cd newdir
ls
cat filex
cd
cd fusepy
fusermount -u fusemount








