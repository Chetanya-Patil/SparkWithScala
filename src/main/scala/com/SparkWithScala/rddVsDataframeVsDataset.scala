package com.SparkWithScala

import org.apache.spark.sql.SparkSession

object rddVsDataframeVsDataset {

  def main(args: Array[String]): Unit = {

    // -- RDD

    // 1. This low level code is not developer friendly
    // 2. RDD lacks some of the basic optimizations.

    // -- DATAFRAME

    // 1. Dataframe do not offer strongly type code...
    //    type errors wont be caught at compile time  rather we get surprise at runtime

    // 2. Developers felt that there flexibility has become limited - Not flexible to use low level code like anonymous function.


    // -- DATASETS
    //  1. Compile time safety
    //  2. We get more flexibility in terms of using lower level code


    val list = List(1,2,3,5,7,6)

    case class ListData(ListElement:String)

    val spark = SparkSession.builder()
      .appName("rddVsDataframeVsDatasets")
      .master("local[2]")
      .getOrCreate()

    val BaseRdd = spark.sparkContext.parallelize(list)

    import spark.implicits._

    val BaseDataframe = BaseRdd.toDF("List_Elements")

    BaseDataframe.printSchema()
    BaseDataframe.show()



    import spark.implicits._

//    //val ListDataset = BaseDataframe.as[ListData]
//
//    val FilterData = ListDataset.filter("List_Elements > 2")
//
//    FilterData.show()


    // DataFrames are more prefferred over Datasets

    /*
       1. When we are dealing with dataframe then the Serialization is managed by tungsten binary format.(Fast)

       2. When we are dealing with datasets then the Serialization is managed by Java Serialization (Slow)

       3. In case of Datasets it has extra cost of casting and expensive serialization.

       4. Datasets - gives compile time safety

       5. Dataframe - Not gives compile time safety
     */

  }

}
