package com.SparkLOWConstruct

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Example8_PlayingWithControlStatement {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def parsline(line:String) = {
    val fields = line.split(" ")
    //val fields = (line.split(" ")(0),line.split(" ")(2).toInt)  // Select First and the Third Element from line.
     if(fields.length == 2)
       {
         (fields(0),fields(1))
       }
     else
       {
         (fields(0),fields(1),fields(2))
       }
  }

  // Or this above function body we can directly pass inside the map using anonymous function
  // .map(x => { Body of Above Function })

  def main(args: Array[String]) : Unit = {

    val sc = new SparkContext("local[2]","Passing Named Function As Parameter")

    val baseRdd = sc.textFile(args(0))

    val rdd1 = baseRdd.map(parsline)

    rdd1.collect.foreach(println)

    scala.io.StdIn.readLine()

  }

}
