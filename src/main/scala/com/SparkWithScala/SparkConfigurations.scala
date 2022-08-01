package com.SparkWithScala

// Important Links
// https://sparkbyexamples.com/spark/spark-get-the-current-sparkcontext-settings/
// https://spark.apache.org/docs/latest/api/scala/org/apache/spark/SparkConf.html

import org.apache.spark.SparkConf
import org.apache.spark.sql._


object SparkConfigurations {

  def main(args:Array[String]):Unit = {

       val sparkConf = new SparkConf()
       sparkConf.set("spark.app.name","SparkApplication")
       sparkConf.set("spark.master","local[2]")

       val spark = SparkSession.builder()
         .config(sparkConf)
         .appName("get sparkContext Configuration")
         .getOrCreate()

       val arrayConfig = spark.sparkContext.getConf.getAll
       for (conf <- arrayConfig){
         println(conf._1 + ", " + conf._2)

         println("Get Particular conf --> " + spark.sparkContext.getConf.get("spark.app.name"))

         for(x <- args)   // For printing the parameters in spark application
           println(x)

         spark.stop()




       }
    }





}
