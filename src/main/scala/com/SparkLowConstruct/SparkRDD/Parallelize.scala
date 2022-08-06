package com.SparkLowConstruct.SparkRDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Parallelize {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark:SparkSession = SparkSession.builder().master("local[2]")
      .appName("Using of Parallelize")
      .getOrCreate()

    val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
    val rddArray: Array[Int] = rdd.collect()

    println(s"Number of partitions: " + rdd.getNumPartitions)
    println(s"Action: First Elements: " + rdd.first())
    println(s"RDD converted to Array[Int] : ")
    rddArray.foreach(println)


    // Create empty rdd

    spark.sparkContext.parallelize(Seq.empty[String])

    scala.io.StdIn.readLine()


  }

}
