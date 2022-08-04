package com.SparkLOWConstruct

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Example4_Placeholder {
  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[2]","Example3")
    for (x <- args) println(x)
    val BaseRdd = sc.textFile(args(0))

    val rdd1 = BaseRdd.flatMap(_.split(" "))

    val lowercaseResult = rdd1.map(_.toLowerCase())

    val rdd2 = lowercaseResult.map((_,1))

    val FinalOutput = rdd2.reduceByKey(_ + _)

    FinalOutput.collect.foreach(println)

    scala.io.StdIn.readLine()


  }

}
