package com.SparkWithScala

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object CreateSparkSession {

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





    spark.stop()
  }

}

