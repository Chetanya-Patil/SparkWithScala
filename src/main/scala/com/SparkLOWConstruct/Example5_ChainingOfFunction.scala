package com.SparkLOWConstruct

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Example5_ChainingOfFunction {
  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[2]","Example3")
    for (x <- args) println(x)

    val OutputWithChaining = sc.textFile(args(0)).flatMap(_.split(" ")).map(_.toLowerCase()).map((_,1)).reduceByKey(_ + _)

    OutputWithChaining.collect.foreach(println)

    scala.io.StdIn.readLine()


  }

}
