# Streaming Task Tutorial

## Scenario

In this tutorial a data flow set up to simulate CDC on a RDBMS database (MYSQL). Afterwards, CDC data written into a KAFKA topic to be an available stream source for downstream systems.
Finally, stream data read by Spark Structured Streaming read data methods and aggregate functions executed to follow and find result of simple analytical query.(Detecting how many clients registered from which store)

With combination of 3 structure mentioned above a seamless, non-interrupted data read-write operations executed as long as new data inserted into RDBMS database.
Below flow diagram illustrates what has done over as a summary.


