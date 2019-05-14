# SCALE-UP

__Ability to increase/decrease underlying host compute resources (CPU/RAM/Storage) dynamically without database downtime.__

__SA Maintainer__: [Brian Leonard](mailto:brian.leonard@mongodb.com) <br/>
__Time to setup__: 15 mins <br/>
__Time to execute__: 30 mins <br/>


## Description

This proof shows how MongoDB can effortlessly scale up while maintaining application uptime and therefore business continuity.

Specifically, the proof will show that a sequential insert process will not miss an entry while its cluster is reconfigured to a larger size in terms of CPU, RAM and Storage. Throughout the scaling process the cluster's three nodes will be monitored while the cluster goes through its dynamic expansion reconfiguration. 

## Setup
__1. Configure Laptop__
* Ensure Python3 is installed and install required Python libraries:
  ```bash
  pip3 install pymongo asyncio
  ```
* [Download](https://www.mongodb.com/download-center/compass) and install Compass on your laptop

__2. Configure Atlas Environment__
* Log-on to your [Atlas account](http://cloud.mongodb.com) (using the MongoDB SA preallocated Atlas credits system) and then choose either to use an existing project or to create a new project
* In the project's Security tab, choose to add a new user called __main_user__, and for __User Privileges__ specify __Read and write to any database__ (make a note of the password you specify)
* Create an __M20__ based 3 node replica-set in a single cloud provider region of your choice with with default settings
* In the Security tab, add a new __IP Whitelist__ for your laptop's current IP address
* In the Atlas console, for the database cluster you deployed, click the __Connect button__, select __Connect Your Application__, and for the __latest Python version__  copy the __Connection String Only__ - make a note of this MongoDB URL address to be used later

__3. Start the Monitoring Script__
* To configure the connection URL for the various Python scripts that will be run, copy the Application Short SRV connection string you just recorded and set this as the value of the _conn\_string_ field in the _params.py_ file in the base folder of this proof (changing the text _\<PASSWORD\>_ with the password you set for _main\_user_ earlier)
  ```
  # Input parameters
  conn_string = 'mongodb+srv://main_user:MyPassword@testcluster-abcde.mongodb.net/test?retryWrites=true'
  ```
* From a terminal/shell in the base folder of this proof, execute the _monitor.py_ monitoring script which establishes a connection to each of the 3 cluster nodes and reports on which node is primary along with the host's memory size and number of records inserted - this enables you to watch in real time the status of all the nodes in the cluster:
  ```bash
  ./monitor.py
  ```
![](img/monitoring.png)

&nbsp;&nbsp;&nbsp;&nbsp;If there was existing records in the database from a prior execution, run _reset\_data.py_ to first delete the collection
    

## Execution
* Ensure the _monitor.py_ script is still running and remains running for the duration of this proof
* In a separate terminal/shell, in the base folder of this proof, execute the _insert\_data.py, watching in real time as data is inserted and then replicated to the secondaries
  ```bash
  ./insert_data.py
  ```
![](img/insert_data.png)

* While the workload is running, via the __Atlas Console__, choose __Edit Configuration__ for the cluster. change cluster tier __from M20 to M30__ and click __Apply Changes__ to scale-up the cluster. Keep watching the monitoring script output as the cluster scales up over the next few minutes in a rolling update fashion, beginning with one of the secondaries
![](img/interrupted.png)

After a few minutes the first upgraded secondary will automatically come back online. notice the RAM displayed has increased accordingly and the records have caught up with the other nodes:

![](img/interrupted2.png)

In the final step of the automated scale-up process, the primary node will be stepped-down and one of the recently upgraded secondaries will be elected primary, which will look like this in the monitoring window:

![](img/election.png)

The monitoring window is updated very 1/2 second and you'll notice from the previous screenshots that about 10-15 records get inserted every 0.5 seconds. However, during the failover, that's a slight pause at record 16619, where no records are inserted for a couple of seconds whilst the current primary is automatically stepped down and another replica automatically becomes the primary. The remarkable thing here is how quickly MongoDB is able to elect a new primary node (in a couple of seconds), resulting in an almost imperceptible affect on the application.  Once the primary is elected, inserts continue automatically. 

Note, the driver is able to gracefully deal with the primary election because it is using the [Retryable Writes](https://docs.mongodb.com/manual/core/retryable-writes/) feature of MongoDB 3.6

__IMPORTANT__: If you want to prove scale down, just re-run the _Execution_ steps again but this time changing the cluster tier in the Atlas console from __M30 to M20__
 

## Measurement
During the scale-up process, watch the output of the running __monitor.py__ script and observe the RAM reserved for each 3 replicas in the cluster increase from 4GB to 8GB. 

Also, once the database has scaled-up, verify the database workload didn't miss any inserts. Make a note of the record number from the _monitor.py_ output, just before the primary was stepped down, which in our screenshot example is 16617.

Using Compass, for the database collection _mydb.records_, filter for records near our election window, for example:

```{val:{$gt:16617}}```

and sort on val:

![](img/records.png)

Verify no record inserts were lost during the scale-up operation

