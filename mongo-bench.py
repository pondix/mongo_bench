#!/usr/bin/python
import os, sys
import datetime
import string
import random
import time
import json
import pymongo
import uuid
from multiprocessing import Process, Manager
from pymongo import MongoClient

def cleardb(_connstr, _database):
  client = MongoClient(_connstr)
  db = client[_database]
  db.connection.drop_database(_database)

def run_inserts(manager_list, _connstr, _database, _collection, _id, str_cnt):
  client = MongoClient(_connstr)
  db = client[_database]
  collection = db[_collection]
  post = { "date": datetime.datetime.utcnow(),
           "docset_id": _id}
  for iter in range(97,97+str_cnt):
      post[chr(iter)] = str(uuid.uuid4())
  # Debug insert results:
  # print post
  start_time = time.time()
  post_id = collection.insert(post)
  manager_list.append(time.time() - start_time)

def run_reads(manager_list, _connstr, _database, _collection, _id):
  client = MongoClient(_connstr)
  db = client[_database]
  collection = db[_collection]
  start_time = time.time()
  r_doc = collection.find_one({"docset_id": _id})
  # Debug find results:
  # print "Document returned: " + str(r_doc)
  manager_list.append(time.time() - start_time)

def exec_insert_test(connstr, database, collection, documents, benchmarks, concurrency, min_id, max_id, str_cnt):
  print "Running write test (insert):"
  benchmark_stats = []
  manager = Manager()
  for benchmark in range(benchmarks):
  #  print 'Benchmark #' + str(benchmark)
    br_collection = collection + "_" + str(benchmark)
    seq_ids = []
    manager_list = manager.list()
    for n in range(documents):
      seq_ids.append(random.randint(min_id, max_id))
    id_start = 0
    id_offset = concurrency
    while ( id_start < documents ):
      sys.stdout.write('.')
      processlist = []
      for rid in seq_ids[id_start:(id_offset + id_start)]:
        processlist.append(Process(target = run_inserts, args = (manager_list, connstr, database, br_collection, rid, str_cnt)))
      for p in processlist:
        p.start()
      for p in processlist:
        p.join()
      id_start += id_offset
      for p in processlist:
        p.terminate()
    benchmark_stats.append(sum(manager_list)/float(len(manager_list)))
    time.sleep(1.0)
  qtiming = sum(benchmark_stats)/float(benchmarks)
  print '*'
  print 'WRITE TEST RESULTS (insert):'
  print 'Number of documents: ' + str(documents)
  print 'Concurrency: ' + str(concurrency)
  print 'Number of benchmarks: ' + str(benchmarks)
  print 'Average connection & query time: ' + str(qtiming)
  print 'Average QPS: ', str(60/qtiming)

def exec_read_test(connstr, database, collection, documents, benchmarks, concurrency, min_id, max_id):
  print "Running read test (find):"
  benchmark_stats = []
  manager = Manager()
  for benchmark in range(benchmarks):
  #  print 'Benchmark #' + str(benchmark)
    br_collection = collection + "_" + str(benchmark)
    random_ids = []
    manager_list = manager.list()
    for n in range(documents):
      random_ids.append(random.randint(min_id, max_id))
    id_start = 0
    id_offset = concurrency
    while ( id_start < documents ):
      sys.stdout.write('.')
      processlist = []
      for rid in random_ids[id_start:(id_offset + id_start)]:
        processlist.append(Process(target = run_reads, args = (manager_list, connstr, database, br_collection, rid)))
      for p in processlist:
        p.start()
      for p in processlist:
        p.join()
      id_start += id_offset
      for p in processlist:
        p.terminate()
    benchmark_stats.append(sum(manager_list)/float(len(manager_list)))
    time.sleep(1.0)
  qtiming = sum(benchmark_stats)/float(benchmarks)
  print '*'
  print 'READ TEST RESULTS (find):'
  print 'Number of documents: ' + str(documents)
  print 'Concurrency: ' + str(concurrency)
  print 'Number of benchmarks: ' + str(benchmarks)
  print 'Average connection & query time: ' + str(qtiming)
  print 'Average QPS: ', str(60/qtiming)

''' Options '''
database = 'mongobench'
collection = 'testcollection'
documents = 100000 #1024
benchmarks = 1
concurrency = 128 # 64
min_id = 0
max_id = documents # 500
connstr = "mongodb://localhost:28017/"
str_cnt = 1 # number of UUID string elements in document
#host = sys.argv[1]
#user = 'mongo-bench'
#password = 'mongo-bench'

cleardb(connstr, database)
exec_insert_test(connstr, database, collection, documents, benchmarks, concurrency, min_id, max_id, str_cnt)
exec_read_test(connstr, database, collection, documents, benchmarks, concurrency, min_id, max_id)
