# Streaming Task Tutorial

## Scenario
Analytics department would like to see distribution of customer registration operations by store in real time. Target is to see how many customer being registered from a store in time windows of ... minute intervals.

To simulate solution for above requirement, In this tutorial a data flow set up to simulate CDC on a RDBMS database (MYSQL). Afterwards, CDC data written into a KAFKA topic to be an available stream source for downstream systems.
Finally, stream data read by Spark Structured Streaming read data methods and aggregate functions executed to follow and find result of simple analytical query.(Detecting how many clients registered from which store in specific time window)

With combination of 3 structure mentioned above a seamless, non-interrupted data read-write operations executed as long as new data inserted into RDBMS database.
Below flow diagram illustrates what has done over as a summary.

![picture alt](spark_stream_job/drawio/flow_diagram.jpg) 
