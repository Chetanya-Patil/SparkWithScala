package com.SparkWithScala.SparkSQL_CreateDataFrame

import org.apache.spark.sql.SparkSession

object fromRDD {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[2]").appName("dataframefromrdd")
      .getOrCreate()

    val columns = Seq("lanaguage","user_count")
    val data = Seq(("Java","20000"),("Python","100000"),("Scala","3000"))

    // 1. Create dataframe from RDD

    val rdd = spark.sparkContext.parallelize(data)

    import spark.implicits._
    val dataframe = rdd.toDF()  // give default column as _1, _2
    dataframe.printSchema()

    val dataframefromrdd = rdd.toDF("language","user_count")
    dataframefromrdd.printSchema()


    // 2. Create Dataframe from createDataFrame()

    val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
    dfFromRDD2.printSchema()





  }

}
