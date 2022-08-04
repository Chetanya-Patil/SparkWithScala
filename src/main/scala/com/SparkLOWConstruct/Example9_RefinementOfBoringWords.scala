package com.SparkLOWConstruct

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Example9_RefinementOfBoringWords {
  def main(args: Array[String]): Unit  = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[2]","Refinement of boring words")

    val baseRdd = sc.textFile(args(0))

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
    // And Understand the OOPs part of scala to understand properly.



  }

}
