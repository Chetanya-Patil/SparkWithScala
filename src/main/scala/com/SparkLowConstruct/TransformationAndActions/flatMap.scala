package com.SparkLowConstruct.TransformationAndActions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object flatMap {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[2]","map transformations which describe one to one mapping")

    val baseRdd = sc.textFile(args(0))

    val flatMapOutput = baseRdd.flatMap(x => x.split(" "))

    // Here, Horizontal line containing multiple words is split with space in-between the words and list down one by one.

    // Example
    // Big Data training

    // will be converted to

    // Big
    // Data
    // training

    flatMapOutput.collect.foreach(println)

    scala.io.StdIn.readLine()


  }

}
