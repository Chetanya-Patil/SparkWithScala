package com.SparkLowConstruct.TransformationAndActions


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object map {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[2]","map transformations which describe one to one mapping")

    val baseRdd = sc.textFile(args(0))

    val mapOutput = baseRdd.map(x => (x,1))

    // Here, Above in map transformation we mapping one to one and converting the x value x,1

    mapOutput.collect.foreach(println)

    scala.io.StdIn.readLine()

  }

}
