package com.SparkLowConstruct

import org.apache.spark.sql.SparkSession

object Example12_ConvertRDDToDataframes {
  def main(args:Array[String]):Unit = {

    val spark = SparkSession.builder()
      .appName("Convert rdd to dataframe")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val columns = Seq("language","users_count")
    val data = Seq(("Java",2000),("Python",100000),("Scala",200000))

    val rdd = spark.sparkContext.parallelize(data)

    val dfFromRDD1 = rdd.toDF()
    dfFromRDD1.printSchema()

    // By default, toDF() function creates column names as “_1” and “_2” like Tuples.

    // toDF() has another signature that takes arguments to define columns.

    val dfFromRDD2 = rdd.toDF("language","users_counts")
    dfFromRDD2.printSchema()


    // Convert RDD to DataFrame - Using createDataFrame()

    val dfFromRDD3 = spark.createDataFrame(rdd).toDF(columns:_*)
    dfFromRDD3.printSchema()
    dfFromRDD3.show(false)

    // Convert RDD to Dataset

  }

}
