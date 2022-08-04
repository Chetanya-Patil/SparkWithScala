package com.SparkLOWConstruct

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Example7_PassingNamedFunctionAsParameter {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def parsline(line:String) = {
    val fields = line.split(" ")
    val FirstElement = fields(0)     // if element is in string and want to convert it in int then we can do that.
    val SecondElement = fields(1)
    val ThirdElement = fields(2)
    (FirstElement,SecondElement,ThirdElement)
  }

  def main(args: Array[String]) : Unit = {

    val sc = new SparkContext("local[2]","Passing Named Function As Parameter")

    val baseRdd = sc.textFile(args(0))

    val rdd1 = baseRdd.map(parsline)

    rdd1.collect.foreach(println)

    scala.io.StdIn.readLine()

  }

}
