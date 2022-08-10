package com.Spark_DataSource_API

import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadAndWriteCSVfile {

  def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder()
        .appName("Read and Write CSV file")
        .master("local[2]")
        .getOrCreate()

      val Data = spark.read
        .format("csv")
        .option("inferSchema",true)
        .option("path",args(0))
        .option("header",true)
        .load()

      Data.show()

      Data.write
        .format("csv")
        .option("path",args(1))
        .save()

    //  .partitionBy("order_status")
    //  .mode(SaveMode.Overwrite)



    // Work on this again


    spark.stop()





  }

}
