package com.SparkLowConstruct.Reading_Files

import org.apache.spark.SparkContext

object Example1_Text {

  def parseline(line: String) = {

    val fields = line.split(",")
    val first = fields(0).toInt
    val second = fields(1)
    val third = fields(2)
    val fourth = fields(3)
    val fifth = fields(4).toInt
    (first,second,third,fourth,fifth)
  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[2]","Reading Text file in RDD")

    val BaseRdd = sc.textFile(args(0))

    val output = BaseRdd.map(parseline)

    output.collect.foreach(println)

    sc.stop()

  }

}
