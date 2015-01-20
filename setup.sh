#!/bin/sh

## Load data to HDFS
dse hadoop fs -mkdir webhdfs://sandbox.hortonworks.com:50070/user/guest/data
dse hadoop fs -copyFromLocal data/sftmax.csv webhdfs://sandbox.hortonworks.com:50070/user/guest/data/sftmax.csv

## Create Keyspace and Table and load data
cqlsh -e "CREATE KEYSPACE IF NOT EXISTS spark_ex2 WITH REPLICATION = { 'class':'SimpleStrategy', 'replication_factor':1}"
cqlsh -e "DROP TABLE IF EXISTS spark_ex2.sftmin"
cqlsh -e "CREATE TABLE IF NOT EXISTS spark_ex2.sftmin(location TEXT, year INT, month INT, day INT, tmin DOUBLE, datestring TEXT, PRIMARY KEY ((location), year, month, day)) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC)"
cqlsh -e "COPY spark_ex2.sftmin(location, year, month, day, tmin, datestring) FROM 'data/sftmin.csv'"

