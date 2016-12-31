#!/usr/bin/env python

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
from sys import argv, exit

''' A class to represent any node (file/directory) present in the filesystem'''
class Node():
  def __init__(self, path, nodetype, metadata, node_id):        
    self.path = path
    self.children = []
    self.nodetype = nodetype
    self.metadata = metadata
    self.node_id = node_id
    self.blocks_version = []

# Presents a HT interface
class MetaServer:
  def __init__(self):
    self.data = {}
    self.fd = 0
    self.node_unique_id = 0

  def count(self):
    return len(self.data)

  def is_alive(self, msg):
    return msg

  # Returns a unique id for every new node created in the file system
  def get_new_id(self):
    self.node_unique_id = self.node_unique_id + 1
    return self.node_unique_id

  # Returns the node id (integer) of the given absolute path name (string)
  def get_node_id(self, path):
    nodeid = -1
    # If the key is in the data structure, return properly formated results
    if path in self.data:
      node = pickle.loads(self.data[path])
      nodeid = node.node_id
    return nodeid

  # Returns a new file descriptor
  def get_new_fd(self):
    self.fd = self.fd + 1
    return self.fd

  # Retrieve something from the HT
  def get(self, key):
    # Default return value
    rv = "None--Empty"
    # If the key is in the data structure, return properly formated results
    key = key.data
    if key in self.data:
      rv = Binary(self.data[key])
    return rv

  # Insert something into the HT
  def put(self, key, value):
    # Remove expired entries
    self.data[key.data] = value.data
    return True

  # Delete something from the HT
  def delete(self, key):
    key = key.data
    if key in self.data:
      del self.data[key]
      return True
    else:
      return False	

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return True
 
  def print_node(self, path): 
   if path in self.data:
      node = pickle.loads(self.data[path])
      print(path)
      print(node.blocks_version)

def main():
  port = int(argv[1])
  serve(port)

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  ms = MetaServer()
  file_server.register_function(ms.count)
  file_server.register_function(ms.is_alive)
  file_server.register_function(ms.get)
  file_server.register_function(ms.put)
  file_server.register_function(ms.print_content)
  file_server.register_function(ms.print_node)
  file_server.register_function(ms.delete)
  file_server.register_function(ms.get_new_id)
  file_server.register_function(ms.get_node_id)
  file_server.register_function(ms.get_new_fd)
  file_server.serve_forever()

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

if __name__ == "__main__":
  main()
