#!/usr/bin/env python3
import pymongo, sys, random, params

connection = pymongo.MongoClient(params.conn_string)
print('Inserting records continuously....')
connect_problem = False

# Drop the existing collection
connection.mydb.records.drop()

val = 1 # The first record, which will match our count in looper.js

while True:
    try:
        connection.mydb.records.insert_one({'val': val});
        val += 1	

        if (val % 100 == 0):
            print (val,  'records inserted')

        if (connect_problem):
            print('Reconnected')
            connect_problem = False
    except KeyboardInterrupt:
        print
        sys.exit(0)
    except:
        print('\n********\n\nConnection problem\n\n********\n')
        connect_problem = True

