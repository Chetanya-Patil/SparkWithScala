package com.SparkLowConstruct.TransformationAndActions

import org.apache.spark.SparkContext

object SortByKey_Example {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[2]","Sortbykey example")

    val baseRdd = sc.textFile(args(0))

    val rdd1 = baseRdd.flatMap(x => x.split(" "))

    val rdd2 = rdd1.map(x => (x,1))

    val rdd3 = rdd2.reduceByKey(_ + _)

    val rdd4 = rdd3.map(x => (x._2,x._1))

    val rdd5 = rdd4.sortByKey(false)

    rdd4.collect.foreach(println)


    // Has to check this code again


    scala.io.StdIn.readLine()
  }

}
