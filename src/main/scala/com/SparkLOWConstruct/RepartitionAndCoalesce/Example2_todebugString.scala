package com.SparkLOWConstruct.RepartitionAndCoalesce

import org.apache.spark.SparkContext

object Example2_todebugString {

  def main(args: Array[String]):Unit = {

    val sc = new SparkContext("local[2]","To get lineage graph")

    val baseRdd = sc.textFile(args(0))

    val rdd1 = baseRdd.flatMap(x => x.split(" "))

    val rdd2 = rdd1.map(x => (x,1))

    val rdd3 = rdd2.reduceByKey(_ + _)

    println(s"This is leanage graph of rdd " + rdd3.toDebugString)
    rdd3.collect.foreach(println)

    scala.io.StdIn.readLine()

  }

}
