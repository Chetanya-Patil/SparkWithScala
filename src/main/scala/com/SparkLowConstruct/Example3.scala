package com.SparkLowConstruct

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Example3 {
  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[2]","Example3")
    for (x <- args) println(x)
    val BaseRdd = sc.textFile(args(0))

    val rdd1 = BaseRdd.flatMap(x => x.split(" "))

    val lowercaseResult = rdd1.map(x => x.toLowerCase())

    val rdd2 = lowercaseResult.map(x => (x,1))

    val FinalOutput = rdd2.reduceByKey((a,b) => a + b)

    FinalOutput.collect.foreach(println)

    scala.io.StdIn.readLine()


  }

}
