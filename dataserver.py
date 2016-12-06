#!/usr/bin/env python

import os, sys, copy, SimpleXMLRPCServer, shelve, getopt, pickle, time, threading, xmlrpclib, unittest, socket
from datetime import datetime, timedelta
from xmlrpclib import Binary
from sys import argv, exit

REPLICATION = 2

def is_server_alive(connection):
  print ('at is_server_alive of dataserver.py')
  msg = 12345
  res = -1
  try:
    res = connection.is_alive(msg)
    if res == msg:
      return True
  except socket.error:
    print "Server is down!"
  return False

# Presents a HT interface
class DataServer:

  def __init__(self, sid, dataservers):
    print ('at init of Dataserver with id',int(sid))
    self.serv_id = sid
    serv_port = dataservers[int(sid)]
    ds_filename = "datastore_"+str(serv_port)
    self.dataservers = dataservers
    #print("printing dataservers list:",dataservers)
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

    # Defining an array of dictionaries called data, which would store the original and redundant block contents
    self.data = []
    for i in range(REPLICATION):
      dsfile = ds_filename+"_"+str(i)
      d = None
      msg = "Testing"
      if os.path.isfile(dsfile) is False and is_server_alive(self.next_conn) and is_server_alive(self.prev_conn):
	if i == 0:
	  d = self.next_conn.get_dict(1) 
        elif i == 1:
	  d = self.prev_conn.get_dict(0)
        if d is not None:
	  self.data.insert(i, shelve.open(dsfile, writeback=True))
	  d = d.data
	  d = pickle.loads(d)
	  for key in d.keys():
	    self.data[i][key] = d[key]
	  self.data[i].sync()
      else:
	self.data.insert(i, shelve.open(dsfile, writeback=True))
      

  # An additional param (index) has been used in all supported RPC methods to update corresponding data dictionary
  def count(self, index):
    return len(self.data[index])

  def is_alive(self, msg):
    print('at is_alive in dataserver.py')
    return msg

  # Retrieve something from the HT
  def get(self, index, key):
    print ('at get () in dataserver.py')
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
    print ('at put () in dataserver.py')
    self.data[index][key.data] = value.data
    self.data[index].sync()
    return True

  # Delete something from the HT
  def delete(self, index, key):
    print ('at delete () in dataserver.py')
    key = key.data
    if key in self.data[index]:
      del self.data[index][key]
      self.data[index].sync()
      return True
    else:
      return False

  # Delete all entries of a file i.e. fileid here
  def delete_key_contains(self, key_contains):
    print ('at get_key_contains () in dataserver.py')
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
    print ('at corrupt_file () in dataserver.py')
    corrupt_str = "$Corrupt$FILE$"
    file_str = str(fileid)+"%%%"
    for i in range(REPLICATION):
      for key in self.data[i].keys():
        if file_str in key:
	  blk_tuple = pickle.loads(self.data[i][key])
          blk_data = blk_tuple[0][:6]+corrupt_str+blk_tuple[0][6+len(corrupt_str):]
	  blk_csum = blk_tuple[1]
	  blk_info = (blk_data, blk_csum)
          self.data[i][key] = pickle.dumps(blk_info)	
      	  self.data[i].sync()
	  print "!! ALERT - File corrupted - fileId :", fileid, "and block# :", key.split(file_str)[1]
	  return True
    return False

  # Correct the corresponding block, by fetching from its replica
  def correct(self, index, key):
    print ('at correct () in dataserver.py')
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
    print ('at print_content () in dataserver.py')
    for i in range(REPLICATION):
      print("Content in Dictionary ", i)
      print self.data[i]
    return True

  # Share complete dictionary (index) with adjacent servers
  def get_dict(self, index):
    print("Sharing the dictionary", index)
    print self.data[index]
    d = dict(self.data[index])
    res = Binary(pickle.dumps(d))
    return res
  
  def terminate(self):
    global quit
    quit=1
    return 1


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
  file_server.register_function(ds.terminate)
  file_server.register_function(ds.get)
  file_server.register_function(ds.put)
  file_server.register_function(ds.print_content)
  file_server.register_function(ds.delete)
  file_server.register_function(ds.delete_key_contains)
  file_server.register_function(ds.corrupt_file)
  file_server.register_function(ds.correct)
  file_server.register_function(ds.get_dict)
  file_server.serve_forever()

if __name__ == "__main__":
  main()
