#!/usr/bin/env python3
import pymongo, sys, random, params

connection = pymongo.MongoClient(params.conn_string)
print('Dropping the records collection....')

# Drop the existing collection
connection.mydb.records.drop()


