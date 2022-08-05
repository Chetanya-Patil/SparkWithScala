package com.SparkLOWConstruct

// Here in Broadcast shuffle is not involved.
// Official Documentation: https://spark.apache.org/docs/latest/api/scala/org/apache/spark/broadcast/Broadcast.html

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Example9_RefinementOfBoringWords {

  def loadboringWords(): Set[String] ={
    val boringWords:Set[String] = Set("in","the","is","of","to")
    boringWords
  }

  def main(args: Array[String]): Unit  = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[2]","Refinement of boring words")

    val nameset = sc.broadcast(loadboringWords)

    val baseRdd = sc.textFile(args(0))

    val rdd1 = baseRdd.flatMap(x => x.split(" "))

    val rdd2 = rdd1.filter(x => !nameset.value(x))

    rdd2.collect.foreach(println)


    // So Here we will discuss about refinement of boring words
    // OUTPUT
    /*
        (data,450)
        (big,600)
        (in,250)
        (hadoop,400)
        (course,100)

     */

    // So, Here A word "In" is a boring word so suppose with this output data we want build wordCloud and we don't
    // want to include such boring words.

    // Refinement of boring words

    // Just insert the link of official documentation here and research about it completely.


    scala.io.StdIn.readLine()

  }

}
