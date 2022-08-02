package com.SparkLOWConstruct

// Important links - official documentation
// https://spark.apache.org/docs/latest/api/scala/org/apache/spark/rdd/index.html

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Example2 {
  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[2]","Example2")

    val MyAppName = sc.getConf.getAll
    println(MyAppName.foreach(println))

    val baseRdd = sc.textFile("C:\\Users\\chaitanya_patil\\IdeaProjects\\SparkWithScala\\src\\main\\scala\\com\\inputtext2")

    val rdd1 = baseRdd.flatMap(x => x.split(" "))

    val rdd2 = rdd1.map(x => (x,1))

    val FinalOutput = rdd2.reduceByKey((a,b) => a + b)

    FinalOutput.collect.foreach(println)   //Action call -- collect


    scala.io.StdIn.readLine()
  }

}
