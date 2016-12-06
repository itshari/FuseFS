#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging
import xmlrpclib
import pickle
import math

from xmlrpclib import Binary

def main():
  metaport=raw_input("Enter metaserver port")
  
  
  connection_meta= xmlrpclib.ServerProxy('http://localhost:'+metaport)
  connection_data= xmlrpclib.ServerProxy('http://localhost:3333')
  
  file_path=raw_input('Enter the path for the file to be corrupted')
  nodeid=connection_meta.get_node_id(file_path)
  print('The node_id for the file is:', nodeid)
  connection_data.corrupt_file(nodeid)
    

if __name__ == '__main__':
  main()
