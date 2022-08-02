package com.SparkWithScala

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext


object SparkContextVsSparkSession {


//
//  The confusion is historical.
//  Spark 0.7.3 version program starts with SparkContext.
//    See the example.

  val sc = SparkContext("local", "Job Name")
  val data_rdd = sc.textFile(logFile)

  //SparkContext stored cluster details, offered core API, and didn't allow SQL those days.

  // Later, Spark 1.0 introduced Spark SQL using SQLContext. See the example.

  val sc = SparkContext("local", "Job Name")
  val sqlContext = SQLContext(sc)

  val teens_rdd = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

  // They also allowed running hive SQL. But for that, you must create a HiveContext. See the example.



  val sc = SparkContext("local", "Job Name")
  val hiveContext = HiveContext(sc)

  val results_rdd = hiveContext.hql("SELECT key, value FROM tbl")

 // The point is straight.
 // Spark Context was at the core. We always needed to create Spark Context for connecting to the cluster and use the RDD APIs.

 //   In Spark 2.0, they combined Hive Context and SQL Context into one object and named it as SparkSession. But we still needed SparkContext.
 //   See the example.


  val sc = SparkContext("local", "Job Name")
  val spark = SparkSession(sc)

  val df = spark.read.csv("filePath\")

   //  So the SparkContext is the main object that establishes the connection to the Spark cluster and provides RDD API.

   // The SparkSession is a combined object of SQLContext and HiveContext, offering you Data Frame and SQL functionality. And it takes SparkContext as an input so it can connect to the cluster and use RDD APIs internally.

   // The above code was clumsy because we needed to create SparkContext and then pass it to create SparkSession. There cames Spark Session Builder. The Session Builder will internally create the SparkContext and manage cluster connection. So the final code looks like this.


  val spark = SparkSession.builder
    .master("local")
    .appName("Job Name")
    .getOrCreate()

  val df = spark.read.csv("filePath\")

//    The above code is the standard approach nowadays. We do not create SparkContext now.
//  We only create SparkSession.
//  But the SparkSession builder will create the SparkContext for us and keep it inside the SparkSession.


}
