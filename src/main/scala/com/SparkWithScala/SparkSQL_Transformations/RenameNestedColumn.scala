package com.SparkWithScala.SparkSQL_Transformations

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object RenameNestedColumn {

  def main(args: Array[String]): Unit = {

      // In Spark withColumnRenamed() is used to rename one column or multiple Dataframe column names.
    /*
      1. Using withColumnRenamed - To rename Spark Dataframe column name
      2. Using withColumnRenamed - To rename multiple columns
      3. Using StructType - To rename nested column on spark dataframe
      4. Using Select - To rename nested columns
      5. Using withColumn - To rename nested columns
      6. Using col() function - To Dynamically rename all or multiple columns
      7. Using toDF() - To rename all or multiple columns
     */

    val spark = SparkSession.builder().master("local[2]").appName("RenameNestedColumn").getOrCreate()
    import spark.implicits._

    val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
      Row(Row("Michael ","Rose",""),"40288","M",4000),
      Row(Row("Robert ","","Williams"),"42114","M",4000),
      Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )

    val schema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)


    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    df.printSchema()


    // 1. Using Spark withColumnRenamed - To rename Dataframe column name

    // Syntax - def withColumnRenamed(existingName: String, newName: String): DataFrame

    df.withColumnRenamed("dob","DateOfBirth").printSchema()

    // 2. Using withColumnRenamed - To rename multiple columns

    val df2 = df.withColumnRenamed("dob","DateOfBirth")
      .withColumnRenamed("salary","salary_amount")
    df2.printSchema()


    // 3. Using Spark StructType - To rename a nested column in Dataframe

    val schema2 = new StructType()
      .add("fname",StringType)
      .add("middlename",StringType)
      .add("lname",StringType)

    df.select(col("name").cast(schema2),
      col("dob"),
      col("gender"),
      col("salary")).printSchema()

    // 4. Using Select - To rename nested elements

    df.select(col("name.firstname").as("fname"),
        col("name.middlename").as("mname"),
        col("name.lastname").as("lname"),
        col("dob"),col("gender"),col("salary")).printSchema()

    // 5. Using Spark Dataframe withColumn - To rename nested columns

         // When you have nested columns on Spark Dataframe and if you want to rename it, use withColumn on a data frame object to create
          //a new column from existing and we will need to drop the existing column.

    val df4 = df.withColumn("fname",col("name.firstname"))
      .withColumn("mname",col("name.middlename"))
      .withColumn("lanem",col("name.lastname"))
      .drop("name")

    df4.printSchema()

    // 6. Using col() function - To Dynamically rename all or multiple columns

      // Another way to change all column names on Dataframe is to use col() function.

    val old_columns = Seq("dob","gender","salary","fname","mname","lanem")
    val new_columns = Seq("DataOfBirth","Sex","salary","firstName","middleName","lastName")
    val columnList = old_columns.zip(new_columns)  // List((dob,DataOfBirth), (gender,Sex), (salary,salary), (fname,firstName), (mname,middleName), (lname,lastName))
      .map(x => {col(x._1).as(x._2)})

    val df5 = df4.select(columnList:_*)
    df5.printSchema()
    println(columnList)

    // 7. Using toDF() - To change all columns in a spark Dataframe

    // Note: When we have data in a flat structure (without Nested), use toDF() with a new schema to change all column names.

    val newColumns = Seq("newCol1","newCol2","newCol3")
    val df9 = df.toDF(newColumns:_*)









  }

}
