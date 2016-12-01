#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

import xmlrpclib
import sys
import pickle
import math

from xmlrpclib import Binary
from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

BLOCK_SIZE = 512
REPLICATION = 2

''' Class to handle the file content as an array of strings '''
class Filecontent():
    def __init__(self, node, ds):
	if node != -1:
            self.path = node.path
	    self.node_id = node.node_id 
	    self.file_size = node.metadata['st_size']        
	    self.ds = ds
	    self.ds_start_index = hash(self.path)%len(self.ds)
	    self.num_blocks = int(math.ceil(self.file_size/BLOCK_SIZE)) 
	else:
	    print("No such file exists!")
	    raise FuseOSError(ENOENT)	
	     

    ''' This function traverses through the blocks and writes the given data at the particular offset '''
    def write_into_blocks(self, data, offset): 
	blk_index = offset//BLOCK_SIZE
	pos = offset%BLOCK_SIZE
	while (len(data) > 0):
	    current_content = self.get_block(blk_index) 
	    if (current_content == -1): 
		current_content =  ""
	    if(data == '\x00' and offset < self.file_size):
        	self.set_block(blk_index, current_content[:pos] + data)
		for i in range(blk_index+1, self.num_blocks): # TODO: Delete remaining blocks from other servers
		    self.delete_block(i)
		break
	    self.set_block(blk_index, (current_content[:pos] + data[:BLOCK_SIZE-pos] + current_content[pos+len(data):]))
	    data = data[BLOCK_SIZE-pos:]
	    blk_index = blk_index + 1
	    pos = 0

    ''' This function traverses through the blocks and reads the given size of data from a particular offset '''
    def read_from_blocks(self, offset, size):
	blk_index = offset//BLOCK_SIZE
	pos = offset%BLOCK_SIZE
	file_cont = ""
	while (blk_index < self.num_blocks):
	    file_cont = file_cont + self.get_block(blk_index)[pos:pos+size]
	    size = size - (BLOCK_SIZE - pos)
	    pos = 0
	    blk_index = blk_index + 1
	return file_cont	

    ''' This function returns the file size as well as the full file content used for debugging '''
    def get_file_size(self):
	size = 0
	cont = ""
	i = 0
	blk = self.get_block(i)
	while (blk != -1):
	    size = size + len(blk)
	    cont = cont + blk
	    i = i+1
	    blk = self.get_block(i)
	return size, cont

    ''' This function fetches the given index of block from the corresponding data server '''
    def get_block(self, index):
	blk = self.ds[(self.ds_start_index+index)%len(self.ds)].get(str(self.node_id)+"%%%"+str(index))
	if (blk != "None--Empty"):
	    return blk.data 
	else:
	    return -1
	
    ''' This function updates the given index of block in the corresponding data server '''
    def set_block(self, index, block_data):  
	for i in range(REPLICATION):
  	    self.ds[(self.ds_start_index+i+index)%len(self.ds)].put((str(self.node_id)+"%%%"+str(index)), block_data)
 
    ''' This function deletes the given index of block from the corresponding data server '''
    def delete_block(self, index):
	return self.ds[(self.ds_start_index+index)%len(self.ds)].delete(str(self.node_id)+"%%%"+str(index)) 
	
    ''' This function deletes the given file from all data servers '''
    def delete_file(self):
	for ds in self.ds:
	    ds.delete_key_contains(str(self.node_id)+"%%%")	
	return self  
	    
	
''' A class to represent any node (file/directory) present in the filesystem'''
class Node():

    def __init__(self, path, nodetype, metadata, node_id):        
	self.path = path
        self.children = []
	self.nodetype = nodetype
        self.metadata = metadata
	self.node_id = node_id
	
    ''' Inserts given child into the list of children of a node'''
    def add_child(self, child):
        if self.nodetype == 0:
	    return self.children.append(child)

    ''' Deletes given child from the node's list of children'''
    def del_child(self,child):
	if self.nodetype == 0:
	    return self.children.remove(child)

class Filesystem():

    def __init__(self, ms):
	self.ms = ms        
	now = time()
        metadata = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
	path = '/'
	nodetype = 0
	node_id = self.ms.get_new_id()
	root_node = Node(path, nodetype, metadata, node_id)
	if (self.get_node(path) == -1):
	    self.add_node(root_node)

    def get_node(self, path):		
	node = self.ms.get(path)
	if node == "None--Empty": 	    
	    print("File or directory not found: ", path)
	    return -1
	else:
	    node = pickle.loads(node.data)
	    return node
	    
    def add_node(self, node):
	path = node.path
	self.ms.put(path, pickle.dumps(node))

    def del_node(self, path):	
	return self.ms.delete(path)

    ''' A member function to propagate the parent directory name change to all its children '''
    def rename_children_with_new_parent(self, node, old_parent, new_parent):	
	for child in node.children:
	    child_node = self.get_node(child)
	  
	    old_child_node = child_node
	    self.del_node(child)
	    old_child_node.path = old_child_node.path.replace(old_parent, new_parent)
	    self.add_node(old_child_node) 
	    
	    _child = child.replace(old_parent,new_parent)
	    node.add_child(_child)
	    node.del_child(child)	
	    self.add_node(node) 
	    
	    self.rename_children_with_new_parent(child_node, old_parent, new_parent)

''' A utility function which splits the last file/dir name from its parent directory'''
def split_parent_and_file(path):
    parent_path, file_name = path.rsplit('/', 1)
    if parent_path == '':
	parent_path = '/'
    return parent_path, file_name

class Server(): 

    def __init__(self, port):
	self.connection = xmlrpclib.ServerProxy('http://localhost:'+port)

    def put(self, key, value):
	self.connection.put(Binary(key), Binary(value))

    def get(self, key):
	return self.connection.get(Binary(key))

    def delete(self, key):
	return self.connection.delete(Binary(key))

    def delete_key_contains(self, key):
	return self.connection.delete_key_contains(Binary(key))

    def get_new_fd(self):
	return self.connection.get_new_fd()

    def get_new_id(self):
	return self.connection.get_new_id()
    
	      
class Memory(LoggingMixIn, Operations):

    def __init__(self):
	self.ms = Server(argv[2])
	self.fs = Filesystem(self.ms)
	self.ds = []
	for i in range(3, len(argv)):
	    self.ds.insert(i-3, Server(argv[i])) 	

    def chmod(self, path, mode):
	node = self.fs.get_node(path)
	node.metadata['st_mode'] &= 0o770000    
        node.metadata['st_mode'] |= mode
	self.fs.add_node(node)
        return 0

    def chown(self, path, uid, gid):
	node = self.fs.get_node(path)
	node.metadata['st_uid'] = uid
        node.metadata['st_gid'] = gid
	self.fs.add_node(node)

    def create(self, path, mode):  
	metadata = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
	node_id = self.ms.get_new_id()
	new_file = Node(path, 1, metadata, node_id)
	parent_path, file_name = split_parent_and_file(path)
	self.fs.add_node(new_file)
	parent_node = self.fs.get_node(parent_path)
	parent_node.add_child(path) 
	self.fs.add_node(parent_node)
        return self.ms.get_new_fd()

    def getattr(self, path, fh=None):
	node = self.fs.get_node(path) 
	print("Path: ", path, " Node:", node)
	if (node == -1) :
            raise FuseOSError(ENOENT)	
        return node.metadata

    def getxattr(self, path, name, position=0):
	attrs = self.fs.get_node(path).metadata.get('attrs', {})
	try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = self.fs.get_node(path).metadata.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode): 
        metadata = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                               st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
	node_id = self.ms.get_new_id()
	node = Node(path, 0, metadata, node_id)	
	parent_path, file_name = split_parent_and_file(path)
	self.fs.add_node(node)
	parent_node = self.fs.get_node(parent_path)
	parent_node.add_child(path)
	parent_node.metadata['st_nlink'] = parent_node.metadata['st_nlink'] + 1
	self.fs.add_node(parent_node)

    def open(self, path, flags): 
	return self.ms.get_new_fd()

    def read(self, path, size, offset, fh):
	node = self.fs.get_node(path)	
	if node.nodetype == 1:
	    filecontent = Filecontent(node, self.ds)   
            return filecontent.read_from_blocks(offset, size)
 
    def readdir(self, path, fh): 
	node = self.fs.get_node(path)
        return ['.','..'] + [split_parent_and_file(x)[1] for x in node.children]

    def readlink(self, path):
	node = self.fs.get_node(path)	
	filecontent = Filecontent(node, self.ds)   
        return filecontent.get_file_size()[1]

    def removexattr(self, path, name):
	node = self.fs.get_node(path)        
	attrs = node.metadata.get('attrs', {})

        try:
            del attrs[name]
	    node.metadata.set('attrs', attrs)
	    self.fs.add_node(node)
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new): # TODO         
	old_parent_path = split_parent_and_file(old)[0]
	new_parent_path = split_parent_and_file(new)[0]

	old_parent_node = self.fs.get_node(old_parent_path)
	new_parent_node = self.fs.get_node(new_parent_path)

	old_node = self.fs.get_node(old)	

	# Renaming all child node's path with old directory to new directory
	self.fs.rename_children_with_new_parent(old_node, old, new)		 

	old_parent_node.children.remove(old)	
	old_node.path = new		
		
	self.fs.add_node(old_node)
	self.fs.add_node(old_parent_node)	
	if old_parent_path == new_parent_path:
	    new_parent_node = old_parent_node
	new_parent_node.add_child(new)
	self.fs.add_node(new_parent_node)	
	self.fs.del_node(old)		

    def rmdir(self, path): # TODO
	node = self.fs.get_node(path)	
	if len(node.children) > 0:
	    print("Trying to delete a non-empty directory: ", path)
	    raise FuseOSError(ENOTEMPTY)	 
	parent_path = split_parent_and_file(path)[0]
	for x in self.fs.get_node(path).children:
	    child_node = self.fs.get_node(x)
	    if child_node.nodetype == 0:
		self.rmdir(x)
	    else:
		self.fs.del_node(x)
		filecontent = Filecontent(child_node, self.ds)
		filecontent.delete_file()
	parent_node = self.fs.get_node(parent_path)
	parent_node.children.remove(path)
	self.fs.del_node(path)
	parent_node.metadata['st_nlink'] = parent_node.metadata['st_nlink'] - 1
	self.fs.add_node(parent_node)

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
	node = self.fs.get_node(path)
        attrs = node.metadata.setdefault('attrs', {})
        attrs[name] = value
	node.metadata.set('attrs', attrs)
	self.fs.add_node(node)

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source): # TODO
	print('Source pathname: ',source)
	print('Target pathname: ',target)
	node_id = self.ms.get_new_id()
	symlink_node = Node(target, 1, dict(st_mode=(S_IFLNK | 0o777), st_nlink=1, st_ctime=time(), 
				st_mtime=time(), st_atime=time(), st_size=len(source)), node_id)
	
	self.fs.add_node(symlink_node)	
	filecontent = Filecontent(symlink_node, self.ds)    
        filecontent.write_into_blocks(source,0)
	parent_path = split_parent_and_file(target)[0]	
	self.fs.add_node(symlink_node)
	parent_node = self.fs.get_node(parent_path)
	parent_node.add_child(target)
	self.fs.add_node(parent_node)

    def truncate(self, path, length, fh=None):
	node = self.fs.get_node(path)	
	filecontent = Filecontent(node, self.ds)    
	data = "\x00"
	offset = length
	file_size = node.metadata['st_size']
	if (length > file_size):
	    offset = file_size
	    data = data*(length-file_size)  
	filecontent.write_into_blocks(data, offset)
        node.metadata['st_size'] = length
	self.fs.add_node(node)

    def unlink(self, path):  
	parent_path = split_parent_and_file(path)[0]
	parent_node = self.fs.get_node(parent_path)
	node = self.fs.get_node(path)
	parent_node.children.remove(path)
	self.fs.add_node(parent_node)
	self.fs.del_node(path)	  
	if node.nodetype == 1:
	    filecontent = Filecontent(node, self.ds)
	    filecontent.delete_file()

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
	node = self.fs.get_node(path)
        node.metadata['st_atime'] = atime
        node.metadata['st_mtime'] = mtime
	self.fs.add_node(node)

    def write(self, path, data, offset, fh):
	node = self.fs.get_node(path)	
	filecontent = Filecontent(node, self.ds)
	filecontent.write_into_blocks(data, offset)
        node.metadata['st_size'] = filecontent.get_file_size()[0]
	self.fs.add_node(node)
        return len(data)

if __name__ == '__main__':
    if len(argv) < 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)

    fuse = FUSE(Memory(), argv[1], foreground=True, debug=True)
