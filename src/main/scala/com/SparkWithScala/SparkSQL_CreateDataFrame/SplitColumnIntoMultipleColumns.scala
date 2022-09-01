package com.SparkWithScala.SparkSQL_CreateDataFrame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}

object SplitColumnIntoMultipleColumns {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("SplitColumnIntoMultipleColumns")
      .getOrCreate()

    val columns = Seq("name","address")
    val data = Seq(("Robert, Smith","1 Main st, Newark, NJ, 92537"),("Maria, Garcia","3456 walnut st, Newark, NJ, 94732"))


    val dfFromData = spark.createDataFrame(data).toDF(columns:_*)
    dfFromData.printSchema()
    dfFromData.show()

    import spark.implicits._

    val newDF = dfFromData.map(x => {

      val nameSplit = x.getAs[String](0).split(",")
      val addressSplit = x.getAs[String](1).split(",")
      (nameSplit(0),nameSplit(1),addressSplit(0),addressSplit(1),addressSplit(2),addressSplit(3))

    })

    val finalDF = newDF.toDF("First Name","Last Name","Address Line1","City","State","zipCode")
    finalDF.printSchema()
    finalDF.show(false)


    // Table of Content
    /*
        1. Split Dataframe Column to multiple columns
        2. Splitting column using withColumn
        3. split dataframe column using raw spark sql
     */

    // 1. Split( str: Column, pattern :String) : Column
    /*
      As you see above, the split() function takes an existing column of dataframe as a first argument and a pattern
      you wanted to split upon as the second argument (this usually is a delimiter) and this function returns an array of Column type.
     */


    val data1 = Seq(("James, A, Smith","2018","M",3000),
      ("Michael, Rose, Jones","2010","M",4000),
      ("Robert,K,Williams","2010","M",4000),
      ("Maria,Anne,Jones","2005","F",4000),
      ("Jen,Mary,Brown","2010","",-1)
    )

    val df = data1.toDF("name","dob_year","gender","salary")
    df.printSchema()
    df.show(false)

    // Using select
    val df2 = df.select(split(col("name"),",").getItem(0).as("FirstName"),
      split(col("name"),",").getItem(1).as("MiddleName"),
      split(col("name"),",").getItem(2).as("LastName"))
      .drop("name")

    df2.printSchema()
    df2.show(false)

    // Splitting column using withColumn

    val splitDF = df.withColumn("FirstName",split(col("name"),",").getItem(0))
      .withColumn("MiddleName",split(col("name"),",").getItem(1))
      .withColumn("LastName",split(col("name"),",").getItem(2))
      .withColumn("NameArray",split(col("name"),","))
      .drop("name")
    splitDF.printSchema()
    splitDF.show(false)


    //Split DataFrame column using raw spark sql

    df.createOrReplaceTempView("PERSON")
    spark.sql("select SPLIT(name,',') as NameArray from PERSON")
      .show(false)



  }

}
