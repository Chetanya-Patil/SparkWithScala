package com.SparkWithScala.SparkSQL_CreateDataFrame

import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

// Spark - How to Change Column Type ?
  /*
      1. Change Column Type using withColumn() and cast()
      2. Using select() to Change Data Type
      3. Using selectExpr() to Change Data Type
      4. Using SQL Expression to convert
   */

object HowToChangeColumnType {

  def main(args: Array[String]):Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("ChangeColumnType")
      .getOrCreate()

      // To change the Spark SQL Dataframe column type from data type to another data type you should use cast() function of Column class,
      // You can use this on withColumn(), select(), selectExpr(), and SQL expression.
      // Note that the type which you want to convert to should be a subclass of DataType class or a string representing the type.


      // When spark unable to convert into a specific type, it returns a null value.
      // DataType
      // ArrayType, BinaryType, BooleanType, CalendarIntervalType, DateType, HiveStringType, MapType, NullType, NumericType, ObjectType, StringType, StructType, TimestampType


    val simpleData  = Seq(Row("James",34,"2006-01-01","true","M",3000.60),
    Row("Michael",33,"1980-01-10","true","F",3300.80),
    Row("Robert",37,"06-01-1992","false","M",5000.50))

    val simpleSchema = StructType(Array(
      StructField("FirstName",StringType,true),
      StructField("age",IntegerType,true),
      StructField("jobStartDate",StringType,true),
      StructField("isGraduated", StringType, true),
      StructField("gender", StringType, true),
      StructField("salary", DoubleType, true)
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData),simpleSchema)
    df.printSchema()
    df.show(false)


    // 1. Change column type using withColumn() and cast()

      val df2 = df.withColumn("age", col("age").cast("String"))
                  .withColumn("jobStartDate",col("jobStartDate").cast(DateType))

      df2.printSchema()
      df2.show()

    // 2. Using select() to change Data Type

    // -- When you have many columns on DataFrame and wanted to cast selected columns this comes handy.
    // -- Use of this will not affect performance

    // Simple form for minimum column
    val cast_df = df.select(col("salary").cast("int").as("salary"))

    // For multiple Columns
    val colum = df.columns   // Gives Array of String
    val cast_df2 = df.select(df.columns.map{
      case column@"age" => col(column).cast("String").as(column)
      case column@"salary" => col(column).cast("String").as(column)
      case column => col(column)
    }:_*)

    cast_df2.printSchema()


    // 3. Using selectExpr() to Change Data Type

    val df3 = df2.selectExpr("cast(age as int) age",
    "cast(jobStartDate as string) jobStartDate")

    df3.printSchema()
    df3.show(false)


    // 4. Using SQL Expression to convert

    df3.createOrReplaceTempView("CastExample")
    val df4 = spark.sql("SELECT STRING(age),BOOLEAN(isGraduated), DATE(jobStartDate) from CastExample")
    df4.printSchema()
    df4.show(false)
























  }

}
