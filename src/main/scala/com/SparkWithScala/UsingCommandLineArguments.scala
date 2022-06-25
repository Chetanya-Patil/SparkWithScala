package com.SparkWithScala

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


object UsingCommandLineArguments {

  def main(args: Array[String]):Unit = {

    // Creating Spark and Configuration

    val sparkconf: SparkConf = new SparkConf()
    sparkconf.set("spark.app.name","CreatingSparkSession")
    sparkconf.set("spark.master","local[2]")

    val spark: SparkSession = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    //val conf = spark.sparkContext.getConf    // Getting configuration from cluster

    println("Standard way to create SparkSession")

    println("Trying to print command line arguments")
    for(x <- args)
    {
      println(x)
    }

    val df = spark.read.text(args(0))        // Argument is defined in run configuration of intellije
    df.show(10)

    val df2 = spark.read.text(args(1))       // Same here second argument is defined in run configuration intellije
    df2.show(5)                    // divided by space





    spark.stop()
  }




}
