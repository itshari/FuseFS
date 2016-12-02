#!/usr/bin/env python

import sys, SimpleXMLRPCServer, shelve, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
from sys import argv, exit

REPLICATION = 2

# Presents a HT interface
class DataServer:
  def __init__(self, sid, dataservers):
    self.serv_id = sid
    ds_filename = "datastore_"+str(self.serv_id)
    self.dataservers = dataservers
    n = len(dataservers)
    if self.serv_id == 0:
      self.prev_conn = xmlrpclib.ServerProxy('http://localhost:'+str(dataservers[n-1])) 
      self.next_conn = xmlrpclib.ServerProxy('http://localhost:'+str(dataservers[self.serv_id+1]))
    elif self.serv_id == n-1:
      self.prev_conn = xmlrpclib.ServerProxy('http://localhost:'+str(dataservers[self.serv_id-1])) 
      self.next_conn = xmlrpclib.ServerProxy('http://localhost:'+str(dataservers[0])) 
    else:
      self.prev_conn = xmlrpclib.ServerProxy('http://localhost:'+str(dataservers[self.serv_id-1])) 
      self.next_conn = xmlrpclib.ServerProxy('http://localhost:'+str(dataservers[self.serv_id+1])) 

    # Defining an array of dictionaries called data, which would store the original and redundanct block contents
    self.data = []
    for i in range(REPLICATION):
      print("index", i)
      self.data.insert(i, shelve.open(ds_filename+"_"+str(i), writeback=True))

  # An additional param (index) has been used in all supported RPC methods to update corresponding data dictionary
  def count(self, index):
    return len(self.data[index])

  def is_alive(self, msg):
    return msg

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
  def delete(self, index, key):
    key = key.data
    if key in self.data[index]:
      del self.data[index][key]
      self.data[index].sync()
      return True
    else:
      return False

  # Delete all entries of a file i.e. fileid here
  def delete_key_contains(self, key_contains):
    key_contains = key_contains.data
    # count to return the number of blocks deleted
    count = 0
    for i in range(REPLICATION):
      for key in self.data[i].keys():
        if key_contains in key:
          del self.data[i][key]
          count = count + 1	
      self.data[i].sync()
    return count

  # Corrupt all entries the dictionaries, key containing the fileid (integer)
  def corrupt_file(self, fileid):
    # count to return the number of blocks corrupted
    count = 0
    corrupt_str = "$Corrupt$FILE$"
    fileid = str(fileid)+"%%%"
    for i in range(REPLICATION):
      for key in self.data[i].keys():
        if fileid in key:
	  blk_tuple = pickle.loads(self.data[i][key])
          blk_data = blk_tuple[0][:6]+corrupt_str+blk_tuple[0][6+len(corrupt_str):]
	  blk_csum = blk_tuple[1]
	  blk_info = (blk_data, blk_csum)
          self.data[i][key] = pickle.dumps(blk_info)
          count = count + 1	
      self.data[i].sync()
    return count

  # Correct the corresponding block, by fetching from its replica
  def correct(self, index, key):
    # Remove expired entries
    d = ""
    print("Correcting the file: ", key.data)
    #TODO: Need to handle this check in a better way
    if index == 0:
      d = self.next_conn.get(index+1, key) 
    elif index == 1:
      d = self.prev_conn.get(index-1, key)
    if d != "None--Empty":
      self.data[index][key.data] = d.data
      self.data[index].sync()
      return True
    return False

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
  dataservers = []
  for i in range(2, len(argv)):
    dataservers.insert(i-2, int(argv[i]))
 
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', dataservers[sid]))
  file_server.register_introspection_functions()
  ds = DataServer(sid, dataservers)
  file_server.register_function(ds.count)
  file_server.register_function(ds.is_alive)
  file_server.register_function(ds.get)
  file_server.register_function(ds.put)
  file_server.register_function(ds.print_content)
  file_server.register_function(ds.delete)
  file_server.register_function(ds.delete_key_contains)
  file_server.register_function(ds.corrupt_file)
  file_server.register_function(ds.correct)
  file_server.serve_forever()

if __name__ == "__main__":
  main()
