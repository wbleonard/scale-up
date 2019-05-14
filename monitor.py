#!/usr/bin/env python3
import pymongo, sys, random, time, asyncio, threading, params
from threading import Thread

# Establish the individual node connetions
def establish_node_connections():

    # Get the credentials from the connection string
    srv_conn_string = params.conn_string
    creds_start = srv_conn_string.find("//")
    creds_end = srv_conn_string.find('@')
    creds = srv_conn_string[creds_start+2:creds_end]
    cluster_members =  cluster_connection.admin.command('replSetGetStatus')['members']

    # connection strings for the individual nodes
    node_connect_strings = []

    for member in cluster_members:
        node_hostname = member['name']
        connect_string = "mongodb://" + creds + "@" + node_hostname + "/test?ssl=true&authSource=admin&retryWrites=true"
        node_connect_strings.append(connect_string)

    node_connections = []
    node_connections.append(pymongo.MongoClient(node_connect_strings[0]))
    node_connections.append(pymongo.MongoClient(node_connect_strings[1]))
    node_connections.append(pymongo.MongoClient(node_connect_strings[2]))

    return node_connections

def get_node_infoT(db, node):

    while True:
        global node1_result
        global node2_result
        global node3_result

        try:

            master = db.command('isMaster')['ismaster']
            status = "PRIMARY" if master else "SECONDARY"

            host_info = db.command("hostInfo")
            mem = int(round(host_info["system"]["memSizeMB"]/float(1000)))

            count = db.records.count_documents({'val': {'$gte': 0}})

            result = status + " (" + str(mem) + "GB)  Records: " + str(count)

            if node == 1:
                node1_result = result
            elif node == 2:
                node2_result = result
            else:
                node3_result = result

        except Exception as e:

            if node == 1:
                node1_result = get_node_status(e, node)
            elif node == 2:
                node2_result = get_node_status(e, node)
            else:
                node3_result = get_node_status(e, node)      

# When a node is down, fetch its last heartbeat message
def get_node_status(e, node):
    try:
        replStatus = cluster_connection.admin.command('replSetGetStatus')
        lastHeartbeatMessage = replStatus['members'][node-1]['lastHeartbeatMessage'] 

        result = str(e) if (lastHeartbeatMessage == "") else lastHeartbeatMessage

        # Truncate the message to keep our output formatted correctly
        truncated_result = (result[:40]) if len(result) > 45 else result
        return truncated_result
    except:
        return "Connection Exception"

def start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

def main(): 

    # Making the cluster connection global so it can easily be used by the status checking threads
    global cluster_connection 

    # These global variables are updated by the status checking threads
    global node1_result
    global node2_result
    global node3_result

    cluster_connection = pymongo.MongoClient(params.conn_string)
    node_connections = establish_node_connections()

    db_node1 = node_connections[0].mydb
    db_node2 = node_connections[1].mydb
    db_node3 = node_connections[2].mydb

    # Create the thread to query the 1st node
    node1_loop = asyncio.new_event_loop()
    node1_loop.call_soon_threadsafe(get_node_infoT, db_node1, 1)
    t = Thread(target=start_loop, args=(node1_loop,))
    t.start()

    # Create the thread to query the 2nd node
    node2_loop = asyncio.new_event_loop()
    node2_loop.call_soon_threadsafe(get_node_infoT, db_node2, 2)
    t = Thread(target=start_loop, args=(node2_loop,))
    t.start()

    # Create the thread to query the 3rd node
    node3_loop = asyncio.new_event_loop()
    node3_loop.call_soon_threadsafe(get_node_infoT, db_node3, 3)
    t = Thread(target=start_loop, args=(node3_loop,))
    t.start()
    
    node1_result = "Fetching status... "
    node2_result = "Fetching status... "
    node3_result = "Fetching status... "

    while True:
        
        print('{0:50} {1:50} {2:50}'.format("Node 1: " + node1_result, "Node 2: " + node2_result, "Node 3: " + node3_result))
        time.sleep(0.5)


if __name__ == "__main__":
    main()





