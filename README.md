# SparkWithScala
Here, In this repo we will discuss spark with scala by sbt

Spark With Scala


Try to Cover all scenario based hands on
Try to use TypeSafe and others things too. 

Spark RDD (Low-Level Construct)

- Spark RDD - Parallelize ( To convert any type of collection to rdd ) ✔
- Spark RDD - Reading Files ✔
- Spark RDD - Create RDD ✔
- Spark RDD - Create empty RDD ✔
- Spark RDD - Transformations ✔
- Spark RDD - Actions ✔
- Spark RDD - Pair Functions ✔
- Spark RDD - Repartition and Coalesce ✔
- Spark RDD - Shuffle Partitions ✔
- Spark RDD - Cache vs Persist ✔
- Spark RDD - Persistance Storage Levels ✔
- Spark RDD - Broadcast Variables ✔
- Spark RDD - Accumulator Variables
- Spark RDD - Convert RDD to Dataframe ✔

Optimization in Low-level Construct (RDD)

- Using Cache and Persist
- Doing broadcast() as per scenario --- Which avoid shuffling
- Adjusting shuffle partitions using coalesce and repartition whenever required.
- File Format Selection
- Serialization
- Always prefer reduceByKey and never use groupByKey
- Try to use mapPartitions() mostly in place of map()

------------------------------------------------------------------------------------------------------------------------

Spark SQL - (High level construct)

- Spark SQL - Create DataFrame ✔
- Spark SQL - Select Columns ✔
- Spark SQL - Add and Update Column (withColumn) ✔
- Spark SQL - How to Change Column Type ✔ -- Using cast() in withColumn, select, selectExpr, sparksql
- Spark SQL - Split Column to Multiple Columns ✔ --Using select, withColumn, spark sql
- Spark SQL - Rename Nested Column ✔
- Spark SQL - Drop Column
- Spark SQL - Where | Filter
- Spark SQL - When Otherwise
- Spark SQL - Collect data to Driver
- Spark SQL - Distinct
- Spark SQL - Pivot Table DataFrame
- Spark SQL - Data Types
- Spark SQL - StructType | StructField
- Spark SQL - Schema
- Spark SQL - Groupby
- Spark SQL - Sort DataFrame
- Spark SQL - Join Types
- Spark SQL - Union and UnionAll
- Spark SQL - map() vs mapPartitions()
- Spark SQL - foreach() vs foreachPartition()
- Spark SQL - map() vs flatMap()
- Spark SQL - Persist and Cache
- Spark SQL - UDF (User Defined Functions)
- Spark SQL - Array (ArrayType) Column
- Spark SQL - Map (MapType) Column
- Spark SQL - Flatten Nested Struct Column
- Spark SQL - Flatten Nested Array Column
- Spark SQL - Explode Array & Map Columns
- Spark SQL - Sampling
- Spark SQL - Partitioning

---------------------------------------------------------------------------

Spark SQL Functions

- Spark SQL String Functions
- Spark SQL Date and Timestamp Functions
- Spark SQL Array Functions
- Spark SQL Map Functions
- Spark SQL Sort Functions
- Spark SQL Aggregate Functions
- Spark SQL Windows Functions
- Spark SQL JSON Functions
