#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging
import xmlrpclib
import pickle
import math

from xmlrpclib import Binary

def main():
  connection_data= xmlrpclib.ServerProxy('http://localhost:4444')
  connection_data.terminate()
  print ('Server 4444 terminated ')
  

if __name__ == '__main__':
  main()
