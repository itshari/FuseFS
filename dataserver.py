#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.03

Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => Binary
      print rv.data => "value"
  put(base64 key, base64 value)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"))
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename)
    Load the contents of the file into the Hahelperable

Changelog:
    0.03 - Modified to remove timeout mechanism for data.
"""

import sys, SimpleXMLRPCServer, shelve, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
from sys import argv, exit

REPLICATION = 2

# Presents a HT interface
class SimpleHT:
  def __init__(self, sid, dataservers):
    self.my_id = sid
    filename = "datastore_"+str(self.my_id)
    self.dataservers = dataservers
    self.data = []
    for i in range(REPLICATION):
      print("index", i)
      self.data.insert(i, shelve.open(filename+"_"+str(i), writeback=True))

  # An additional param (index) has been used in all supported RPC methods to update corresponding data dictionary
  def count(self, index):
    return len(self.data[index])

  # Retrieve something from the HT
  def get(self, index, key):
    # Default return value
    rv = "None--Empty"
    # If the key is in the data structure, return properly formated results
    key = key.data
    if key in self.data[index]:
        rv = Binary(self.data[index][key])
    return rv

  # Insert something into the HT
  def put(self, index, key, value):
    # Remove expired entries
    self.data[index][key.data] = value.data
    self.data[index].sync()
    return True

  # Delete something from the HT
  def delete(self, key):
    key = key.data
    if key in self.data:
      del self.data[key]
      return True
    else:
      return False

  # Delete something from the HT
  def delete_key_contains(self, key_contains):
    key_contains = key_contains.data
    count = 0
    for i in range(REPLICATION):
      for key in self.data[i].keys():
        if key_contains in key:
          del self.data[index][key]
	  self.data[index].sync()
          count = count + 1	
    return count

  # Print the contents of the hashtable
  def print_content(self):
    for i in range(REPLICATION):
      print("Content in Dictionary ", i)
      print self.data[i]
    return True

def main():
  serve()

# Start the xmlrpc server
def serve():
  sid = int(argv[1])
  print("sid:", sid)
  dataservers = []
  for i in range(2, len(argv)):
    dataservers.insert(i-2, int(argv[i]))
 
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', dataservers[sid]))
  file_server.register_introspection_functions()
  sht = SimpleHT(sid, dataservers)
  file_server.register_function(sht.count)
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.delete)
  file_server.register_function(sht.delete_key_contains)
  file_server.serve_forever()

if __name__ == "__main__":
  main()
