package com.Spark_DataSource_API

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object EasiestWayToWorkWithSpark {

  def main(args:Array[String]):Unit = {

    val conf = new SparkConf()
    conf.set("spark.app.name","Easy to work with spark")
    conf.set("spark.master","local[2]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val orderDF = spark.read
      .format("csv")
      .option("header",true)
      .option("inferSchema",true)
      .option("path",args(0))
      .load()

    orderDF.createOrReplaceTempView("orders")

    val output = spark.sql("select * from orders limit 10")

    output.show()

    output.write
      .format("csv")
      .saveAsTable("orderData")

    // while write we can also use saveAsTable to put our data in table form

    // We can also put it in spark metastore  -- spark.sql.warehouse.dir

    // or make connection to external metastore by enableHiveMetastore property while creating SparkSession

    scala.io.StdIn.readLine()

  }

}
