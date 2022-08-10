package com.SparkWithScala

// 3 options to have schema for dataframe
//   1. Inferschema
//   2. Implicit Schema (parquet/avro)
//   3. Explicit Schema


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SchemaForDataframe {

  def main(args: Array[String]): Unit = {

      val schema = StructType(List(
        StructField("SrNO",IntegerType),
        StructField("Name",StringType),
        StructField("City",StringType),
        StructField("Salary",IntegerType)
      ))

      val schema2 = "SrNO Int,Name String,City String,Salary Int"

    val spark = SparkSession.builder()
      .appName("SchemaforDataframe")
      .master("local[2]")
      .getOrCreate()

    val data = spark.read
      .format("csv")
      .schema(schema)                     //This is of schema StructType def schema(schema: StructType)
      .option("path","....")
      .load()

    val data2 = spark.read
      .format("csv")
      .schema(schema2)                  // This schema is of schema string def schema(schemString: String)
      .option("path",".....")
      .load()




  }

}
