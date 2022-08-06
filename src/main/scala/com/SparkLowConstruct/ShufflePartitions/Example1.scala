package com.SparkLowConstruct.ShufflePartitions

import org.apache.spark.SparkContext

object Example1 {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[2]","Example1")

    val baseRdd = sc.textFile(args(0))

    // Is to check the parallelism level
    println(sc.defaultParallelism)

    //To check the number of partitions
    println(baseRdd.getNumPartitions)

    // It determine the minimum number of partitions rdd has when we load from file.
    println(sc.defaultMinPartitions)



    scala.io.StdIn.readLine()
  }

}
