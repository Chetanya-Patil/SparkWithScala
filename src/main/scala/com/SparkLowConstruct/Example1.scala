package com.SparkLowConstruct

// Low-level Construct code in spark with RDD
// Example - wordCount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Example1 {
  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[2]","wordCount")

    val myAppName = sc.getConf.getAll

    println(myAppName.mkString("|"))

    val BaseRdd = sc.textFile("C:\\Users\\chaitanya_patil\\IdeaProjects\\SparkWithScala\\src\\main\\scala\\com\\inputtext")

    val rdd1 = BaseRdd.flatMap(x => x.split(" "))

    val rdd2 = rdd1.map(x => (x,1))

    val FinalCount = rdd2.reduceByKey((a,b) => a + b)

    FinalCount.collect.foreach(println)

    FinalCount.collect.mkString("|")

    scala.io.StdIn.readLine()



  }

}
