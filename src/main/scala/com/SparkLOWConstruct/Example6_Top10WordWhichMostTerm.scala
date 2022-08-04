package com.SparkLOWConstruct

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Example6_Top10WordWhichMostTerm {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[2]","Top 10 words which are most term")

    val BaseRdd = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).map(x => (x._2,x._1))

    val finalOutput = BaseRdd.sortByKey(false)

    val Result = finalOutput.collect

    val ParsedOutput = for(x <- Result)
      {
        val Count = x._1
        val Word = x._2
        println(s"(Word,Count) => : $Word : $Count")
      }

    scala.io.StdIn.readLine()

  }

}
