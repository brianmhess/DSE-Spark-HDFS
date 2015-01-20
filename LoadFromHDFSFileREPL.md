#Overview

The goal of this exercise is to load data from a file on an external HDFS system into a Cassandra table using the DSE Spark REPL.

In this exercise we will perform the following steps:
1. Locate and review the source data in HDFS.
2. Prepare a Cassandra table for the new data.
3. Create a spark RDD from the data in the file and load it into the Cassandra table.
4. Query the table to ensure that the data was correctly loaded.

After this, we will also join data between HDFS and Cassandra and write the results into another Cassandra table.  Those steps are:
1. Locate and review the data in both HDFS and Cassandra.
2. Preapare a Cassandra table for the joined dataset.
3. Create a Spark RDD from the data in the HDFS file.
4. Create a Spark RDD from the data in the Cassandra table.
5. Join the two RDDs together, and write the data into the Cassandra table.
6. Query the table to ensure that the data was correctly loaded.

#Requirements

You need to have a local copy of DSE 4.6 installed (this example is based on a tarball install).

Additionally, you are going to need a Hadoop environment.  A simple way to do this (which is what this exercise is based on) is to download the Hortonworks Sandbox.  The HDP 2.2 Sandbox can be downloaded from  http://hortonworks.com/products/hortonworks-sandbox/.  I used the VMWare version.

#Setup

###1. Install DSE 4.6

Start DSE with Spark enabled (e.g., "dse cassandra -k").  You need to ensure that you can interact with Spark via the REPL (e.g., "dse spark").

###2. Install the Hortonworks HDP 2.2 Sandbox.   
You will need to note the IP address when it starts up.  It will have a notice similar to this:

```
    To initiate your Hortonworks Sandbox session, 
    please open a browser and enter this address 
    in the browser's address field: 
    http://192.168.159.128/
    
    You can access SSH by $ ssh root@192.168.159.128
```
  
You will see that a number of services have been started.  We really only need HDFS and Webhdfs.
Make note of the IP address of this machine.  Mine is 192.168.159.128, and I will use that throughout this exercise.
  
**Note**: Hadoop does most things by hostname, not IP address.  The hostname for the Sandbox is sandbox.hortonworks.com.  Add your Sandbox to /etc/hosts:
    192.168.159.128  sandbox.hortonworks.com

###3. Load data into HDFS on the HDP Sandbox

```
dse hadoop fs -mkdir webhdfs://sandbox.hortonworks.com:50070/user/guest/data
dse hadoop fs -copyFromLocal data/sftmax.csv webhdfs://sandbox.hortonworks.com:50070/user/guest/data/sftmax.csv
```

###4. Load data into Cassandra

```
cqlsh -e "CREATE KEYSPACE IF NOT EXISTS spark_ex2 WITH REPLICATION = { 'class':'SimpleStrategy', 'replication_factor':1}"
cqlsh -e "DROP TABLE IF EXISTS spark_ex2.sftmin"
cqlsh -e "CREATE TABLE IF NOT EXISTS spark_ex2.sftmin(location TEXT, year INT, month INT, day INT, tmin DOUBLE, datestring TEXT, PRIMARY KEY ((location), year, month, day)) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC)"
cqlsh -e "COPY spark_ex2.sftmin(location, year, month, day, tmin, datestring) FROM 'data/sftmin.csv'"
```

# Exercise

We now have data in HDFS (as well as some in Cassandra that we will use later).

##1. Identify the data in HDFS

We can use "dse hadoop fs" to interact with the HDFS file system via Webhdfs.  Webhdfs is a REST interface for HDFS that enables us avoid the requirements around HDFS library compatibility between the client and the server.  Webhdfs does redirect requests to the data nodes, so we need to ensure that every DSE node can route to every HDFS node, and do so via hostname (since Hadoop does most things via hostname and not IP address).

Let's look at the first few lines of the file in HDFS:

```
dse hadoop fs -cat webhdfs://sandbox.hortonworks.com:50070/user/guest/data/sftmax.csv | head
```

You should see something like:

```
ubuntu@ubuntu:~/dev-old/spark/exercise2$ dse hadoop fs -cat webhdfs://sandbox.hortonworks.com:50070/user/guest/data/sftmax.csv | head
15/01/20 11:15:58 INFO snitch.Workload: Setting my workload to Cassandra
USC00047767,2003,01,01,144,20030101
USC00047767,2003,01,02,144,20030102
USC00047767,2003,01,03,167,20030103
USC00047767,2003,01,04,167,20030104
USC00047767,2003,01,05,178,20030105
USC00047767,2003,01,06,194,20030106
USC00047767,2003,01,07,178,20030107
USC00047767,2003,01,08,172,20030108
USC00047767,2003,01,10,156,20030110
USC00047767,2003,01,11,150,20030111
```

Our data is formatted as follows:
* Station ID as a string
* Year
* Month
* Day of Month
* Maximum temperature in 10ths of degree Celsius
* Date String in the format of YYYYMMDD

We can also count the number of lines in the HDFS file via:

```
ubuntu@ubuntu:~/dev-old/spark/exercise2$ dse hadoop fs -cat webhdfs://sandbox.hortonworks.com:50070/user/guest/data/sftmax.csv | wc -l
15/01/20 11:21:51 INFO snitch.Workload: Setting my workload to Cassandra
3606
```

##2. Prepare a Cassandra table for the data

First start the Spark REPL via

```
dse spark
```

We will use the keyspace that we already created during the setup, namely spark_ex2.  From within Spark, we can issue DDL commands to create the necessary table.  First we import the Cassandra Connector:

```
import com.datastax.spark.connector.cql.CassandraConnector
```

Next, we connect to our cassandra cluster

```
val connector = CassandraConnector(csc.conf)
val session = connector.openSession()
```

Now we can issue the DDL commands to drop the table (if it already exists) and create/recreate it:

```
session.execute(s"DROP TABLE IF EXISTS spark_ex2.sftmax")
session.execute(s"CREATE TABLE IF NOT EXISTS spark_ex2.sftmax(location TEXT, year INT, month INT, day INT, tmax DOUBLE, datestring TEXT, PRIMARY KEY ((location), year, month, day)) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC)")
```

Here we are choosing to partition on the LOCATION column and to cluster on the YEAR, MONTH, and DAY column, sorted in descending order.  This puts the newest value at the front.

##3. Create a Spark RDD from the HDFS data and save to Cassandra

Now, let's get that data into Spark and over to Cassandra.  From within the Spark REPL, we will first create a case class to store represent our data:

```
case class Tmax(location: String, year: Int, month: Int, day: Int, tmax: Double, datestring: String)
```

Next, we will read the data into an RDD:

```
val tmax_raw = sc.textFile("webhdfs://192.168.159.128:50070/user/guest/data/sftmax.csv")
```

And then transform that data so that each record of the RDD is in the Tmax type:

```
val tmax_c10 = tmax_raw.map(x=>x.split(",")).map(x => Tmax(x(0), x(1).toInt, x(2).toInt, x(3).toInt, x(4).toDouble, x(5)))
```

We can verify the number of records by calling count:

```
scala> tmax_c10.count
res11: Long = 3606
```

Now, simply store the data into Cassandra via RDD.saveToCassandra():

```
tmax_c10.saveToCassandra("spark_ex2", "sftmax")
```

##4. Verify the data is successfully loaded into Cassandra

We can verify that the data is successfully loaded via a CQL COUNT:

```
scala> session.execute("SELECT COUNT(*) FROM spark_ex2.sftmax").all.get(0).getLong(0)
res23: Long = 3606
```

##5. Join data between HDFS and Cassandra and load into Cassandra

We have a table SPARK_EX2.SFTMIN already loaded into Cassandra.  This has the daily minimum temperature (in tenths of a degree Celsius).  We would like to create a new table that has the daily maximum, minimum, and delta for each day.  We can do this via a simple Join operation.

First, we need to read the data from Cassandra.  We will use another case class here:

```
case class Tmin(location: String, year: Int, month: Int, day: Int, tmin: Double, datestring: String)
val tmin_raw = sc.cassandraTable("spark_ex2", "sftmin")
val tmin_c10 = tmin_raw.map(x => Tmin(x.getString("location"), x.getInt("year"), x.getInt("month"), x.getInt("day"), x.getDouble("tmin"), x.getString("datestring")))
```

In order to join RDDs, they need to be PairRDDs, with the first element in the pair being the join key.  We will transform our RDDs into PairRDDs:

```
val tmin_pair = tmin_c10.map(x=>(x.datestring,x))
val tmax_pair = tmax_c10.map(x=>(x.datestring,x))
```

There is currently an issue in DSE 4.6 with Joins and case classes, so we will remove cases for the join.  In the future, this step will not be needed, but in DSE 4.6 (today) it is:

```
val tmin_pair1 = tmin_pair.map(x=>(x._1, (x._2.location, x._2.year, x._2.month, x._2.day, x._2.tmin, x._2.datestring)))
val tmax_pair1 = tmax_pair.map(x=>(x._1, (x._2.location, x._2.year, x._2.month, x._2.day, x._2.tmax, x._2.datestring)))
```

We will again use a case class for our Hi-Lo-Delta RDD:

```
case class THiLoDelta(location: String, year: Int, month: Int, day: Int, hi: Double, low: Double, delta: Double, datestring: String)
```

Now for the join, and converting that using the THiLoDelta case class:

```
val tdelta_join1 = tmax_pair1.join(tmin_pair1)
val tdelta_c10 = tdelta_join1.map(x => THiLoDelta(x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._2._5, x._2._1._5 - x._2._2._5, x._1))
```

We need to create the Cassandra table to receive this data.  We will again use the SPARK_EX2 keyspace:

```
session.execute(s"DROP TABLE IF EXISTS spark_ex2.sftdelta")
session.execute(s"CREATE TABLE IF NOT EXISTS spark_ex2.sftdelta(location TEXT, year INT, month INT, day INT, hi DOUBLE, low DOUBLE, delta DOUBLE, datestring TEXT, PRIMARY KEY ((location), year, month, day)) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC)")
```

And lastly, let's save our data:

```
tdelta_c10.saveToCassandra("spark_ex2", "sftdelta")
```


