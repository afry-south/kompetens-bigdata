# kompetens-bigdata
Sources and presentation for Kompetensomrade BigData

Pre-load by visiting the downloads page [wiki/Downloads.md]

# Definition of Big Data

`Big data usually includes data sets with sizes beyond the ability of commonly used software tools to capture, curate, manage, and process data within a tolerable elapsed time.` (Snijders, C.; Matzat, U.; Reips, U.-D. (2012). "'Big Data': Big gaps of knowledge in the field of Internet")

The three increasing Vs:
* Volume (amount of data)
* Velocity (speed of data in and out)
* Varitey (range of data types and sources)

# Scaling
_Horizontal scaling_ by adding more server hardware is important, and both volume and velocity should scale linearly with number of servers or nodes.

# Storing data
Data is often stored in a database or in a file system. 

### HDFS
The most common file system is HDFS, which is a component of Hadoop. Each node in the Hadoop cluster contributes with disk space to the HDFS, where a part of the data is stored. All data is also replicated across several nodes (for example 3), to provide redundancy in case of a node failure.

### Databases
Many databases have been developed to manage larger data sets with higher throughput, compared to relational databases. Some examples:
* BigTable (Google)
* HBase (Hadoop)
* Cassandra (Apache)
* MongoDB

# Processing data
Hadoop is the dominating framework to help scaling or distributing the processing of big data. There are two patterns being used:
* MapReduce
* Spark

### MapReduce high-level frameworks
Nowadays, you rarely develop pipelines based on the low-level MapReduce API. Instead, you use other tools, frameworks or languages, such as
* Crunch, a java Spark or MapReduce pipeline builder framework
* Pig, high-level data-flow language and execution framework for parallel computation
* Hive, provides data summarization and ad hoc querying
* 

# Sending data
Kafka is a commonly used Message Bus, to which you can connect producers and consumers. Each consumer is guaranteed to receive all messages being produced, even though if the network connection is interrupted.

Two common use cases of Kafka:
* collecting log data from front end servers, and write the logs to a HDFS
* Real-time processing of streams with high bandwidths, e.g. web site analytics

### Serializing data
Common formats of serialized big data are
* Avro (Apache Hadoop) supports schema evolution
* ProtoBuf (Google)
* JSON (JavaScript Object Notation)
* 
